package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpointGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import com.ibm.disni.RdmaServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RamcastAgent {
  private static final Logger logger = LoggerFactory.getLogger(RamcastAgent.class);
  RamcastConfig config = RamcastConfig.getInstance();

  private RamcastEndpointGroup endpointGroup;

  private RamcastNode node;
  private RdmaServerEndpoint<RamcastEndpoint> serverEp;

  private RamcastNode leader;
  private MessageDeliveredCallback onDeliverCallback;

  public RamcastAgent(int groupId, int nodeId) throws Exception {
    this(
        groupId,
        nodeId,
        new MessageDeliveredCallback() {
          @Override
          public void call(Object data) {
            if (RamcastConfig.LOG_ENABLED) logger.debug("Delivered message {}", data);
          }
        });
  }

  public RamcastAgent(int groupId, int nodeId, MessageDeliveredCallback onDeliverCallback)
      throws Exception {
    MDC.put("ROLE", groupId + "/" + nodeId);
    this.node = RamcastNode.getNode(groupId, nodeId);
    this.onDeliverCallback = onDeliverCallback;

    // temp for setting a leader;
    this.leader = RamcastNode.getNode(groupId, 0);
  }

  public void bind() throws Exception {
    // listen for new connections
    this.endpointGroup = RamcastEndpointGroup.createEndpointGroup(this, config.getTimeout());
    this.serverEp = endpointGroup.createServerEndpoint();
    this.serverEp.bind(this.node.getInetAddress(), 100);
    Thread listener =
        new Thread(
            () -> {
              MDC.put("ROLE", node.getGroupId() + "/" + node.getNodeId());
              while (true) {
                try {
                  RamcastEndpoint endpoint = this.serverEp.accept();
                  while (!endpoint.isReady()) Thread.sleep(10);
                  if (RamcastConfig.LOG_ENABLED)
                    logger.debug(
                        ">>> Server accept connection of endpoint {}. CONNECTION READY", endpoint);
                } catch (IOException | InterruptedException e) {
                  e.printStackTrace();
                  logger.error("Error accepting connection", e);
                }
              }
            });
    listener.setName("ConnectionListener");
    listener.start();
  }

  public void establishConnections() throws Exception {

    this.endpointGroup.connect();
    while (true) {
      Thread.sleep(10);
      // todo: find nicer way for -1
      if (this.getEndpointMap().keySet().size() != config.getTotalNodeCount()) continue;
      if (this.getEndpointMap().values().stream()
          .map(RamcastEndpoint::isReady)
          .reduce(Boolean::logicalAnd)
          .get()) break;
    }

    if (this.isLeader()) {
      this.endpointGroup.requestWritePermission();
    }

    this.endpointGroup.startPollingData();

    while (true) {
      Thread.sleep(10);
      // todo: find nicer way for -1
      if (this.getEndpointMap().keySet().size() != config.getTotalNodeCount()) continue;
      if (this.getEndpointMap().values().stream()
          .map(RamcastEndpoint::hasExchangedPermissionData)
          .reduce(Boolean::logicalAnd)
          .get()) break;
    }

    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
          "Agent of node {} is ready. EndpointMap: Key:{} Vlue {}",
          this.node,
          this.getEndpointMap().keySet(),
          this.getEndpointMap().values());
  }

  public Map<RamcastNode, RamcastEndpoint> getEndpointMap() {
    return this.endpointGroup.getNodeEndpointMap();
  }

  public RamcastNode getNode() {
    return node;
  }

  public RamcastEndpointGroup getEndpointGroup() {
    return endpointGroup;
  }

  @Override
  public String toString() {
    return "RamcastAgent{" + "node=" + node + '}';
  }

  public void multicast(RamcastMessage message, List<RamcastGroup> destinations)
      throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("Multicasting to dest {} message {}", destinations, message);
    for (RamcastGroup group : destinations) {
      this.endpointGroup.writeMessage(group, message.toBuffer());
    }
  }

  public void multicast(ByteBuffer buffer, List<RamcastGroup> destinations) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.debug("Multicasting to dest {} buffer {}", destinations, buffer);
    for (RamcastGroup group : destinations) {
      this.endpointGroup.writeMessage(group, buffer);
    }
  }

  public RamcastMessage createMessage(ByteBuffer buffer, List<RamcastGroup> destinations) {
    int[] groups = new int[destinations.size()];
    destinations.forEach(group -> groups[destinations.indexOf(group)] = group.getId());
    RamcastMessage message = new RamcastMessage(buffer, groups);
    int msgId = Objects.hash(this.node.getOrderId());
    message.setId(msgId);
    short[] slots = getRemoteSlots(destinations);
    while (slots == null) {
      Thread.yield();
      slots = getRemoteSlots(destinations);
    }
    message.setSlots(slots);
    return message;
  }

  private short[] getRemoteSlots(List<RamcastGroup> destinations) {
    short[] ret = new short[destinations.size()];
    int i = 0;
    for (RamcastGroup group : destinations) {
      int tail = -1;
      int available = 0;
      List<RamcastEndpoint> eps = endpointGroup.getGroupEndpointsMap().get(group.getId());
      for (RamcastEndpoint ep : eps) {
        if (ep.getAvailableSlots() <= 0) return null;
        tail = ep.getRemoteCellBlock().getTailOffset();
        break;
        //        if (tail < ep.getRemoteCellBlock().getTailOffset()
        //            && (available < ep.getAvailableSlots())) {
        //          tail = ep.getRemoteCellBlock().getTailOffset();
        //          available = ep.getAvailableSlots();
        //        }
      }
      ret[i++] = (short) tail;
    }
    return ret;
  }

  public void close() throws IOException, InterruptedException {
    if (RamcastConfig.LOG_ENABLED) logger.info("Agent of {} is closing down", this.node);
    this.serverEp.close();
    this.endpointGroup.close();
    RamcastGroup.close();
  }

  public void setLeader(RamcastNode node) throws IOException {
    this.leader = node;
    // if this is the new leader
    if (this.leader.equals(this.node)) {
      // need to revoke permission of old leader
      this.getEndpointGroup().revokeTimestampWritePermission();
    }
  }

  public boolean isLeader() {
    return this.node.equals(leader);
  }

  public void deliver(RamcastMessage message) throws IOException {
    onDeliverCallback.call(message);
    this.endpointGroup.releaseMemory(message);
  }

  public boolean hasClientRole() {
    return this.node.hasClientRole();
  }
}
