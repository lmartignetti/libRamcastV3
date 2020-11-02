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
                        while (true) {
                          if (!isRunning) Thread.yield();
                          if (pendingTimestamps.size() > 0)
                            for (PendingTimestamp pending : pendingTimestamps) {
                              int tmpRound = pending.getRound();
                              int tmpClock = pending.getClock();
                              if (tmpClock <= 0) {
                                Thread.yield();
                                continue;
                              }

                              // set clock
                              group.getClock().set(Math.max(tmpClock, group.getClock().get()));

                              if (RamcastConfig.LOG_ENABLED)
                                logger.debug(
                                        "[{}] receive ts: [{}/{}] of group {}. Local value [{}/{}], pendingTimestamps {}, TS memory: \n {}",
                                        pending.message.getId(),
                                        tmpRound,
                                        tmpClock,
                                        pending.groupId,
                                        group.getRound().get(),
                                        group.getClock().get(),
                                        pendingTimestamps,
                                        group.getTimestampBlock()
                                );

                              // Leader Election
                              if (tmpRound != group.getRound().get()) {
                                if (RamcastConfig.LOG_ENABLED)
                                  logger.trace(
                                          "[{}] timestamp [{}/{}] has babllot doesn't match to local {}",
                                          pending.message.getId(),
                                          tmpRound,
                                          tmpClock,
                                          group.getRound().get());
                                Thread.yield();
                                continue;
                              }

                              // if this is leader, this should propagate the msg to local group if possible
                              if (this.agent.isLeader()) {
                                if (pending.groupId != this.agent.getGroupId() && pending.shouldPropagate()) {
                                  int newClock = group.getClock().incrementAndGet();
                                  if (RamcastConfig.LOG_ENABLED)
                                    logger.debug(
                                            "[{}] remove pending timestamp [{}/{}] of group {} index {}  -- LEADER process propagate ts with value [{}/{}]",
                                            pending.message.getId(),
                                            tmpRound,
                                            tmpClock,
                                            pending.groupId,
                                            pending.groupIndex,
                                            tmpRound,
                                            newClock);
                                  group.leaderPropageTs(
                                          pending.message,
                                          tmpRound,
                                          newClock,
                                          pending.groupId,
                                          pending.groupIndex);
                                  pending.shouldPropagate = false;
                                  Thread.yield();
                                  continue;
                                }
                              }


//                              if (tmpSequence == group.getCurrentSequenceNumber().get() + 1) {
//                              if (tmpSequence == pending.getSequenceNumber()) {
//                                group.getCurrentSequenceNumber().incrementAndGet();
//                                group.getSequenceNumber().set(tmpSequence);
//                                if (!this.agent.isLeader()) group.getClock().set(tmpClock);
                              if (RamcastConfig.LOG_ENABLED)
                                logger.debug(
                                        "[{}] remove pending timestamp [{}/{}] of group {} index {}",
                                        pending.message.getId(),
                                        tmpRound,
                                        tmpClock,
                                        pending.groupId,
                                        pending.groupIndex);
                              if (pending.shouldAck() && !pending.isSendAck() && !this.agent.isLeader()) {
                                if (RamcastConfig.LOG_ENABLED)
                                  logger.debug("[{}] Start sending ack for timestamp [{}/{}] of group {} index {}",
                                          pending.message.getId(),
                                          tmpRound,
                                          tmpClock,
                                          pending.groupId,
                                          pending.groupIndex);
                                group.sendAck(pending.message, tmpRound, tmpClock, pending.groupId, pending.groupIndex);
                                pending.sendAck = true;
                              }
                              pendingTimestamps.remove(pending);
//                              }
                            }

                          int minTs = Integer.MAX_VALUE;
                          if (processing.size() > 0) {
                            for (RamcastMessage message : processing) {
                              if (isFulfilled(message)) {
                                message.setFinalTs(group.getTimestampBlock().getMaxTimestamp(message));
                                logger.debug("[{}] finalts={} is fulfilled. BEFORE Remove from processing, put to order. processing={}, order={}", message.getId(), message.getFinalTs(), processing, ordered);
                                this.ordered.add(message);
                                this.processing.remove(message);
                                logger.debug("[{}] finalts={} is fulfilled. AFTER Remove from processing, put to order. processing={}, order={}", message.getId(), message.getFinalTs(), processing, ordered);
                              }
                              if (minTs > message.getFinalTs()) minTs = message.getFinalTs();
                            }
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
    if (!message.isAcked(group.getRound().get())) {
      if (RamcastConfig.LOG_ENABLED)
        logger.debug("[{}] fulfilled: NO - doesn't have enough ACKS {}", message.getId(), message);
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
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("Handling message {} ts block", message);
    int msgId = message.getId();
    if (RamcastConfig.LOG_ENABLED) logger.debug("[Recv][Step #1][msgId={}]", msgId);
    if (agent.isLeader()) {
      if (RamcastConfig.LOG_ENABLED) logger.debug("[{}] Leader processing...", msgId);
      try {
        group.writeTimestamp(message, group.getRound().get(), group.getClock().incrementAndGet());
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

  public ConcurrentSkipListSet<RamcastMessage> getOrdered() {
    return ordered;
  }

  public void setRunning(boolean running) {
    isRunning = running;
  }

  private class PendingTimestamp {
    boolean shouldAck;
    boolean shouldPropagate = true;
    int groupId;
    int groupIndex;
    RamcastMessage message;
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
    }

    public int getRound() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset);
    }

    public int getClock() {
      return group.getTimestampBlock().getBuffer().getInt(timestampOffset + 4);
    }

    public String toString() {
      return "id=" + message.getId() + ":[" + getRound() + "/" + getClock() + "]";
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
