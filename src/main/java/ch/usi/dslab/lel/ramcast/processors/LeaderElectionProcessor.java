package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class LeaderElectionProcessor {
  private static final Logger logger = LoggerFactory.getLogger(LeaderElectionProcessor.class);
  RamcastAgent agent;
  RamcastEndpointGroup group;
  AtomicInteger acks;

  public LeaderElectionProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
    this.agent = agent;
    this.group = group;
    this.acks = new AtomicInteger(0);
  }

  public void handleLeaderElectionMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
      throws IOException {
    int ticket = buffer.getInt(0);
    if (ticket == RamcastConfig.MSG_HS_C_GET_WRITE) { // msg step 1 sent from client
      int groupId = buffer.getInt(4);
      int nodeId = buffer.getInt(8);
      int ballotNumnber = buffer.getInt(12);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[HS] Step Request Write permission Server Receiving: [{}/{}], ballot={}",
            groupId,
            nodeId,
            ballotNumnber);

      // if request comes from different group => don't need to check ballot number.
      // other wise have to check
      // or request from that group itself
      if ((groupId == this.group.getAgent().getNode().getGroupId()
              && nodeId == this.group.getAgent().getNode().getNodeId())
          || (groupId != this.group.getAgent().getNode().getGroupId())
          || (this.group.getBallotNumber().get() < ballotNumnber)) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
              "[HS] received ballot [{}] greater than current ballot [{}]. Granting permission.",
              ballotNumnber,
              this.group.getBallotNumber());

        if (this.group.getBallotNumber().get() < ballotNumnber
            && groupId == this.group.getAgent().getNode().getGroupId()) {
          this.group.setBallotNumber(ballotNumnber);
        }
        // if request comes from same group => need to revoke permission of other nodes
        if (groupId == this.group.getAgent().getNode().getGroupId())
          this.group.revokeTimestampWritePermission();

        // set that node as leader
        this.agent.setLeader(groupId, nodeId);

        RamcastMemoryBlock memoryBlock = endpoint.registerTimestampWritePermission();
        ByteBuffer response = ByteBuffer.allocateDirect(20);
        response.putInt(RamcastConfig.MSG_HS_S_GET_WRITE);
        response.putLong(memoryBlock.getAddress());
        response.putInt(memoryBlock.getLkey());
        response.putInt(memoryBlock.getCapacity());
        endpoint.send(response);
      }
      endpoint.setHasExchangedPermissionData(true);
    } else if (ticket == RamcastConfig.MSG_HS_S_GET_WRITE) { // msg step 1 sent from client
      endpoint.setRemoteSharedTimestampMemoryBlock(
          buffer.getLong(4), buffer.getInt(12), buffer.getInt(16));
      acks.getAndIncrement();
      endpoint.setHasExchangedPermissionData(true);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[HS] Step Request Write permission CLIENT Receiving from {}: timestampBlock addr={} lkey={} capacity={} ack={}",
            endpoint.getNode(),
            buffer.getLong(4),
            buffer.getInt(12),
            buffer.getInt(16),
            acks.get());
    } else {
      throw new IOException("Protocol msg code not found :" + ticket);
    }
  }

  public void requestWritePermission(RamcastEndpoint endpoint, int ballotNumber)
      throws IOException {
    this.acks.set(0);
    ByteBuffer buffer = ByteBuffer.allocateDirect(16);
    buffer.putInt(RamcastConfig.MSG_HS_C_GET_WRITE);
    buffer.putInt(this.agent.getNode().getGroupId());
    buffer.putInt(this.agent.getNode().getNodeId());
    buffer.putInt(ballotNumber);
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
          "[HS] Step Request Write permission CLIENT Sending: to {} with data [{}/{}], ballot={}",
          endpoint.getNode(),
          buffer.getInt(4),
          buffer.getInt(8),
          buffer.getInt(12));
    endpoint.send(buffer);
  }

  public AtomicInteger getAcks() {
    return acks;
  }
}
