package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

public class MessageProcessor {
  private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
  //
  private Queue<RamcastMessage> processing;
  private Queue<PendingTimestamp> pendingTimestamps;
  //    private Queue<RamcastMessage> ordered;
  //
  //  private ConcurrentSkipListSet<RamcastMessage> processing;
  //  private ConcurrentSkipListSet<PendingTimestamp> pendingTimestamps;
  private ConcurrentSkipListSet<RamcastMessage> ordered;

  private RamcastEndpointGroup group;
  private RamcastAgent agent;
  private Thread pendingMessageProcessor;
  private boolean isRunning = true;

  public MessageProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
    this.group = group;
    this.agent = agent;
    //        this.pendingTimestamps = new ConcurrentLinkedQueue<>();
    //        this.processing = new ConcurrentLinkedQueue<>();
    //        this.ordered = new ConcurrentLinkedQueue<>();
    this.pendingTimestamps = new ConcurrentLinkedQueue<>();
    this.processing = new ConcurrentLinkedQueue<>();
    this.ordered = new ConcurrentSkipListSet<>(Comparator.comparingInt(RamcastMessage::getFinalTs));

    this.pendingMessageProcessor =
        new Thread(
            () -> {
              try {
                MDC.put("ROLE", agent.getGroupId() + "/" + agent.getNode().getNodeId());
                while (isRunning) {
                  if (pendingTimestamps.size() > 0)
                    for (PendingTimestamp pending : pendingTimestamps) {
                      int tmpSequence = pending.getSequenceNumber();
                      int tmpBallot = pending.getBallotNumber();
                      int tmpClock = pending.getClockValue();
                      if (tmpSequence <= 0) continue;
                      if (RamcastConfig.LOG_ENABLED)
                        logger.trace(
                            "[{}] receive ts: [{}/{}] of group {}. Local value [{}/{}], pendingTimestamps {} TS memory: \n {}",
                            pending.message.getId(),
                            tmpBallot,
                            tmpSequence,
                            pending.groupId,
                            group.getBallotNumber().get(),
                            group.getCurrentSequenceNumber().get(),
                            pendingTimestamps,
                            group.getTimestampBlock());

                      if (tmpBallot != group.getBallotNumber().get()) {
                        if (RamcastConfig.LOG_ENABLED)
                          logger.trace(
                              "[{}] timestamp [{}/{}] has babllot doesn't match to local {}",
                              pending.message.getId(),
                              tmpBallot,
                              tmpSequence,
                              group.getBallotNumber().get());
                        //                      if (!this.agent.isLeader())
                        continue;
                      }

                      // if this is leader, this should propagate the msg to local group if possible
                      if (this.agent.isLeader()) {
                        if (pending.groupId != this.agent.getGroupId()
                            && pending.shouldPropagate()) {
                          if (RamcastConfig.LOG_ENABLED)
                            logger.debug(
                                "[{}] remove pending timestamp [{}/{}] of group {} index {}, current seqnumber {} -- LEADER process propagate ts with value [{}/{}]",
                                pending.message.getId(),
                                tmpBallot,
                                tmpSequence,
                                pending.groupId,
                                pending.groupIndex,
                                group.getCurrentSequenceNumber().get(),
                                tmpBallot,
                                group.getSequenceNumber().get() + 1);
                          group.leaderPropageTs(
                              pending.message,
                              tmpBallot,
                              tmpClock,
                              pending.groupId,
                              pending.groupIndex);
                          pending.shouldPropagate = false;
                          continue;
                        }
                      }

                      if (tmpSequence == group.getCurrentSequenceNumber().get() + 1) {
                        group.getCurrentSequenceNumber().incrementAndGet();
                        group.getSequenceNumber().set(tmpSequence);
                        if (!this.agent.isLeader()) group.getLocalClock().set(tmpClock);
                        if (RamcastConfig.LOG_ENABLED)
                          logger.debug(
                              "[{}] remove pending timestamp [{}/{}] of group {} index {}, current seqnumber {}",
                              pending.message.getId(),
                              tmpBallot,
                              tmpSequence,
                              pending.groupId,
                              pending.groupIndex,
                              group.getCurrentSequenceNumber().get());
                        if (pending.shouldAck() && !pending.isSendAck() && !this.agent.isLeader()) {
                          logger.debug(
                              "[{}] Start sending ack for timestamp [{}/{}] of group {} index {}, current seqnumber {}",
                              pending.message.getId(),
                              tmpBallot,
                              tmpSequence,
                              pending.groupId,
                              pending.groupIndex,
                              group.getCurrentSequenceNumber().get());
                          group.sendAck(
                              pending.message,
                              tmpBallot,
                              tmpSequence,
                              pending.groupId,
                              pending.groupIndex);
                          pending.sendAck = true;
                        }
                        pendingTimestamps.remove(pending);
                      }
                    }

                  int minTs = Integer.MAX_VALUE;
                  if (processing.size() > 0)
                    for (RamcastMessage message : processing) {
                      if (isFulfilled(message)) {
                        message.setFinalTs(group.getTimestampBlock().getMaxTimestamp(message));
                        this.processing.remove(message);
                        this.ordered.add(message);
                      } else {
                      }
                      if (minTs > message.getFinalTs()) minTs = message.getFinalTs();
                    }

                  if (ordered.size() == 0) continue;
                  int minOrderedTs = Integer.MAX_VALUE;
                  if (ordered.first() != null) minOrderedTs = ordered.first().getFinalTs();

                  for (RamcastMessage message : ordered) {
                    if (message.getFinalTs() <= minTs && message.getFinalTs() <= minOrderedTs) {
                      ordered.remove(message);
                      this.agent.deliver(message);
                    }
                  }

                  Thread.yield();
                }
              } catch (IOException e) {
                e.printStackTrace();
              }
            });
    this.pendingMessageProcessor.setName("MessageProcessor");
    this.pendingMessageProcessor.start();
  }

  private boolean isFulfilled(RamcastMessage message) {
    if (!group.getTimestampBlock().isFulfilled(message)) {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}] fulfilled: NO - doesn't have enough TS", message.getId());
      return false;
    }
    if (!message.isAcked(group.getBallotNumber().get())) {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}] fulfilled: NO - doesn't have enough ACKS {}", message.getId(), message);
      return false;
    }
    // there is a case where ts is not available at the check on line 55, but it is now. so need to
    // check again
    for (PendingTimestamp pending : pendingTimestamps) {
      if (pending.message.getId() == message.getId()) return false;
    }
    return true;
  }

  public void handleMessage(RamcastMessage message) {

    //    try {
    //      agent.deliver(message);
    //      return;
    //    } catch (IOException e) {
    //      e.printStackTrace();
    //    }

    if (RamcastConfig.LOG_ENABLED)
      logger.debug("Handling message {} ts block {}", message, group.getTimestampBlock());
    int msgId = message.getId();
    if (RamcastConfig.LOG_ENABLED) logger.debug("[Recv][Step #1][msgId={}]", msgId);
    if (agent.isLeader()) {
      if (RamcastConfig.LOG_ENABLED) logger.debug("[{}] Leader processing...", msgId);
      try {
        group.writeTimestamp(
            message,
            group.getBallotNumber().get(),
            group.getSequenceNumber().incrementAndGet(),
            group.getLocalClock().incrementAndGet());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    if (RamcastConfig.LOG_ENABLED) logger.debug("[{}] Process processing...", msgId);
    for (int groupIndex = 0; groupIndex < message.getGroupCount(); groupIndex++) {
      PendingTimestamp ts = new PendingTimestamp(message, groupIndex);
      pendingTimestamps.add(ts);
    }
    this.processing.add(message);
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("[{}] pendingTimestamps array {}", msgId, pendingTimestamps);
  }

  //  public ConcurrentSkipListSet<RamcastMessage> getProcessing() {
  public Queue<RamcastMessage> getProcessing() {
    return processing;
  }

  private class PendingTimestamp {
    boolean shouldAck;
    boolean shouldPropagate = true;
    int groupId;
    int groupIndex;
    RamcastMessage message;
    //    ByteBuffer timestampBuffer;
    boolean sendAck = false;
    int msgId;
    int timestampOffset = 0;

    public PendingTimestamp(RamcastMessage message, int groupIndex) {
      this.groupId = message.getGroup(groupIndex);
      this.shouldAck = groupId == agent.getGroupId();
      this.groupIndex = groupIndex;
      this.message = message;
      this.msgId = message.getId();
      this.timestampOffset = group.getTimestampBlock().getTimestampOffset(message, groupIndex);
      //      this.timestampBuffer =
      //          group.getTimestampBlock().getTimestampBufferOfGroup(message, groupIndex);
    }

    public int getSequenceNumber() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset + 4);
    }

    public int getBallotNumber() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset);
    }

    public int getClockValue() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset + 8);
    }

    public String toString() {
      return "id="
          + message.getId()
          + ":["
          + getBallotNumber()
          + "/"
          + getSequenceNumber()
          + "/"
          + getClockValue()
          + "]";
    }

    public boolean shouldAck() {
      return this.shouldAck;
    }

    public boolean isSendAck() {
      return this.sendAck;
    }

    public boolean shouldPropagate() {
      return this.shouldPropagate;
    }
  }
}
