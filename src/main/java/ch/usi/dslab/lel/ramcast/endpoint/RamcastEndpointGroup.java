package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
import ch.usi.dslab.lel.ramcast.processors.HandshakingProcessor;
import ch.usi.dslab.lel.ramcast.processors.LeaderElectionProcessor;
import ch.usi.dslab.lel.ramcast.processors.MessageProcessor;
import com.ibm.disni.RdmaCqProcessor;
import com.ibm.disni.RdmaCqProvider;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RamcastEndpointGroup extends RdmaEndpointGroup<RamcastEndpoint> {

  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastAgent agent;

  private HashMap<Integer, RamcastCqProcessor<RamcastEndpoint>> cqMap;
  private int timeout;

  // for leader election
  private AtomicInteger currentBallotNumber;

  // storing memory segment block associated with each endpoint
  //  private Map<RamcastEndpoint, RamcastMemoryBlock> endpointMemorySegmentMap;

  // storing all endponints of all nodes
  private Map<RamcastNode, RamcastEndpoint> nodeEndpointMap;
  private Map<Integer, RamcastEndpoint> endpointMap;
  private Map<Integer, List<RamcastEndpoint>> groupEndpointsMap;

  // shared memory for receiving message from clients
  private ByteBuffer sharedCircularBuffer;

  // shared memory for receiving timestamp from leaders
  private ByteBuffer sharedTimestampBuffer;

  private HandshakingProcessor handshakingProcessor;
  private LeaderElectionProcessor leaderElectionProcessor;
  private MessageProcessor messageProcessor;

  private CustomHandler customHandler;

  public RamcastEndpointGroup(RamcastAgent agent, int timeout) throws IOException {
    super(timeout);
    this.timeout = timeout;
    this.agent = agent;
    sharedCircularBuffer =
        allocateSharedBuffer(
            config.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_MESSAGE);
    sharedTimestampBuffer =
        allocateSharedBuffer(
            config.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_TIMESTAMP);

    this.nodeEndpointMap = new ConcurrentHashMap<>();
    this.groupEndpointsMap = new ConcurrentHashMap<>();
    this.cqMap = new HashMap<>();
    this.handshakingProcessor = new HandshakingProcessor(this, agent);
    this.leaderElectionProcessor = new LeaderElectionProcessor(this, agent);
    this.messageProcessor = new MessageProcessor(this, agent);
    this.currentBallotNumber = new AtomicInteger(0);
    //    this.endpointMemorySegmentMap = new ConcurrentHashMap<>();
  }

  public static RamcastEndpointGroup createEndpointGroup(RamcastAgent agent, int timeout)
      throws Exception {
    RamcastEndpointGroup group = new RamcastEndpointGroup(agent, timeout);
    group.init(new RamcastEndpointFactory(group));
    return group;
  }

  public void send(int groupId, int nodeId, ByteBuffer buffer) throws IOException {
    send(RamcastNode.getNode(groupId, nodeId), buffer);
  }

  public void send(RamcastNode node, ByteBuffer buffer) throws IOException {
    this.nodeEndpointMap.get(node).send(buffer);
  }

  public void writeMessage(RamcastNode node, ByteBuffer buffer) throws IOException {
    logger.debug("Write message to {}, buffer {}, ep {}", node, buffer, this.nodeEndpointMap);
    this.nodeEndpointMap.get(node).writeMessage(buffer);
  }

  public void handleProtocolMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
      throws IOException {
    if (buffer.getInt(0) < 0 && buffer.getInt(0) >= -10) { // hack: hs message has ID from -1 -> -10
      this.handshakingProcessor.handleHandshakeMessage(endpoint, buffer);
    } else if (buffer.getInt(0) < -10
        && buffer.getInt(0) >= -20) { // hack: hs message has ID from -11 -> -20
      this.leaderElectionProcessor.handleLeaderElectionMessage(endpoint, buffer);
    }
  }

  public void handleReceive(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleReceive(buffer);
  }

  public void handleSendComplete(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleSendComplete(buffer);
  }

  public void handleReceiveMessage(RamcastMessage message) {
    if (customHandler != null) customHandler.handleReceiveMessage(message);
    this.messageProcessor.handleMessage(message);
  }

  public void handlePermissionError(ByteBuffer buffer) {
    if (customHandler != null) customHandler.handlePermissionError(buffer);
  }

  public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
    this.handshakingProcessor.initHandshaking(endpoint);
  }

  public void requestWritePermission() throws IOException, InterruptedException {
    this.currentBallotNumber.incrementAndGet();
    for (RamcastEndpoint endpoint : this.getNodeEndpointMap().values()) {
      this.leaderElectionProcessor.requestWritePermission(endpoint, this.currentBallotNumber.get());
      logger.debug(">>> Client exchanged permission data to: {}.", endpoint.getNode());
    }
    // wait for receiving acks from all nodes
    while (this.leaderElectionProcessor.getAcks().get()
        // todo: find nicer way for -1
        != RamcastConfig.getInstance().getTotalNodeCount()) Thread.sleep(10);
    logger.debug(
        ">>> Client FINISHED exchanging permission to {} nodes.",
        this.leaderElectionProcessor.getAcks().get());
  }

  public void startPollingData() {
    Map<String, String> contextMap = MDC.getCopyOfContextMap();
    Thread serverDataPolling =
        new Thread(
            () -> {
              MDC.setContextMap(contextMap);
              logger.info("Polling for incoming data");
              while (true) {
                for (Map.Entry<RamcastNode, RamcastEndpoint> e : nodeEndpointMap.entrySet()) {
                  RamcastEndpoint endpoint = e.getValue();
                  if (endpoint != null) {
                    endpoint.pollForData();
                  }
                }
                Thread.yield();
              }
            });
    serverDataPolling.setName("ServerDataPolling");
    serverDataPolling.start();
  }

  public void releaseMemory(RamcastMessage message) throws IOException {
    RamcastMemoryBlock memoryBlock = message.getMemoryBlock();
    int freed = memoryBlock.freeSlot(message.getSlot());
    RamcastEndpoint endpoint = message.getMemoryBlock().getEndpoint();
    if (freed == 0) {
      logger.debug(
          "[{}] Can't release memory slot. There are pending slots", endpoint.getEndpointId());
      return;
    }
    logger.trace(
        "[{}] SERVER MEMORY after releasing memory: {}",
        endpoint.getEndpointId(),
        endpoint.getSharedCellBlock());

    logger.debug(
        "[{}] Released memory of {} slot. Update client [{}].", message.getId(), freed, endpoint);
    message.reset();
    this.writeRemoteHeadOnClient(endpoint, freed);
  }
  //
  //  public void setWritePermission(RamcastEndpoint endpoint) throws IOException {
  //    for (RamcastEndpoint ramcastEndpoint : this.endpointMap.values()) {
  //      ramcastEndpoint.revokeTimestampWritePermission();
  //    }
  //  }

  public void writeTimestamp(
      RamcastEndpoint endpoint, int slotNumber, int ballot, int timestamp, int value)
      throws IOException {
    RamcastMemoryBlock timestampBlock = endpoint.getRemoteTimeStampBlock();
    long address = timestampBlock.getAddress() + slotNumber * RamcastConfig.SIZE_TIMESTAMP;
    endpoint.writeSignal(
        address,
        timestampBlock.getLkey(),
        Integer.TYPE,
        ballot,
        Integer.TYPE,
        timestamp,
        Integer.TYPE,
        value);
  }

  public void writeRemoteHeadOnClient(RamcastEndpoint endpoint, int headOffset) throws IOException {
    RamcastMemoryBlock clientBlock = endpoint.getRemoteServerHeadBlock();
    endpoint.writeSignal(clientBlock.getAddress(), clientBlock.getLkey(), Integer.TYPE, headOffset);
  }

  public ByteBuffer getSharedCircularBuffer() {
    return sharedCircularBuffer;
  }

  public ByteBuffer getSharedTimestampBuffer() {
    return sharedTimestampBuffer;
  }

  public void setCustomHandler(CustomHandler customHandler) {
    this.customHandler = customHandler;
  }

  public Map<RamcastNode, RamcastEndpoint> getNodeEndpointMap() {
    return nodeEndpointMap;
  }

  public int getBallotNumber() {
    return currentBallotNumber.get();
  }

  public Map<Integer, List<RamcastEndpoint>> getGroupEndpointsMap() {
    return groupEndpointsMap;
  }

  public RamcastAgent getAgent() {
    return agent;
  }

  private ByteBuffer allocateSharedBuffer(int connectionCount, int queueLength, int packageSize) {
    int capacity = connectionCount * queueLength * packageSize;
    return ByteBuffer.allocateDirect(capacity);
  }

  protected synchronized IbvQP createQP(RdmaCmId id, IbvPd pd, IbvCQ cq) throws IOException {
    IbvQPInitAttr attr = new IbvQPInitAttr();
    attr.cap().setMax_recv_wr(config.getQueueLength() * 10);
    attr.cap().setMax_send_wr(config.getQueueLength() * 10);
    attr.cap().setMax_recv_sge(1);
    attr.cap().setMax_send_sge(1);
    attr.cap().setMax_inline_data(config.getMaxinline());
    attr.setQp_type(IbvQP.IBV_QPT_RC);
    attr.setRecv_cq(cq);
    attr.setSend_cq(cq);
    return id.createQP(pd, attr);
  }

  @Override
  public RdmaCqProvider createCqProvider(RamcastEndpoint endpoint) throws IOException {
    logger.trace("setting up cq processor");
    IbvContext context = endpoint.getIdPriv().getVerbs();
    if (context != null) {
      logger.trace("setting up cq processor, context found");
      RamcastCqProcessor<RamcastEndpoint> cqProcessor;
      int key = context.getCmd_fd();
      if (!cqMap.containsKey(key)) {
        int cqSize = (config.getQueueLength() * 2) * 3 * config.getTotalNodeCount();
        cqProcessor =
            new RamcastCqProcessor<>(
                context, cqSize, config.getQueueLength(), 0, 1, this.timeout, config.isPolling());
        cqMap.put(context.getCmd_fd(), cqProcessor);
        cqProcessor.start();
      }
      cqProcessor = cqMap.get(context.getCmd_fd());

      return cqProcessor;
    } else {
      throw new IOException("setting up cq processor, no context found");
    }
  }

  @Override
  public IbvQP createQpProvider(RamcastEndpoint endpoint) throws IOException {
    IbvContext context = endpoint.getIdPriv().getVerbs();
    RamcastCqProcessor<RamcastEndpoint> cqProcessor = cqMap.get(context.getCmd_fd());
    IbvCQ cq = cqProcessor.getCQ();
    IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
    logger.trace("registering endpoint with cq");
    cqProcessor.registerQP(qp.getQp_num(), endpoint);
    return qp;
  }

  @Override
  public void allocateResources(RamcastEndpoint endpoint) throws Exception {
    endpoint.allocateResources();
  }

  public void close() throws IOException, InterruptedException {
    super.close();
    for (RamcastEndpoint endpoint : nodeEndpointMap.values()) {
      endpoint.close();
    }
    for (RamcastCqProcessor<RamcastEndpoint> cq : cqMap.values()) {
      cq.close();
    }
    logger.info("rpc group down");
  }

  public void connect() throws Exception {
    // trying to establish a bi-direction connection
    // A node only connect to node with bigger ids.
    logger.debug("ALL nodes {}", RamcastGroup.getAllNodes());
    for (RamcastNode node : RamcastGroup.getAllNodes()) {
      if (node.getOrderId() >= this.agent.getNode().getOrderId()) {
        logger.debug("connecting to: {}", node);
        RamcastEndpoint endpoint = this.createEndpoint();
        endpoint.connect(node.getInetAddress(), config.getTimeout());
        endpoint.setNode(node);
        this.initHandshaking(endpoint);
        while (!endpoint.isReady()) Thread.sleep(10);
        logger.debug(">>> Client connected to: {}. CONNECTION READY", node);
      }
    }
  }

  public void setBallotNumber(int ballotNumnber) {
    this.currentBallotNumber.set(ballotNumnber);
  }

  public void revokeTimestampWritePermission() throws IOException {
    for (RamcastEndpoint ramcastEndpoint : this.getNodeEndpointMap().values()) {
      // only revoke permission of nodes in same group
      // and are not leader
      if (ramcastEndpoint.getNode().getGroupId() == this.agent.getNode().getGroupId() && !ramcastEndpoint.getNode().isLeader()) {
        logger.debug(
            "Revoking write permission of {} on {}",
            ramcastEndpoint.getNode(),
            this.agent.getNode());
        ramcastEndpoint.registerTimestampReadPermission();
      }
    }
  }

  public static class RamcastEndpointFactory implements RdmaEndpointFactory<RamcastEndpoint> {
    private RamcastEndpointGroup group;

    public RamcastEndpointFactory(RamcastEndpointGroup group) {
      this.group = group;
    }

    @Override
    public RamcastEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
      return new RamcastEndpoint(group, id, serverSide);
    }
  }

  private static class RamcastCqProcessor<C extends RamcastEndpoint> extends RdmaCqProcessor<C> {
    Map<String, String> contextMap = MDC.getCopyOfContextMap();

    public RamcastCqProcessor(
        IbvContext context,
        int cqSize,
        int wrSize,
        long affinity,
        int clusterId,
        int timeout,
        boolean polling)
        throws IOException {
      super(context, cqSize, wrSize, affinity, clusterId, timeout, polling);
    }

    @Override
    public void dispatchCqEvent(RamcastEndpoint endpoint, IbvWC wc) throws IOException {
      if (contextMap != null) {
        MDC.setContextMap(contextMap); // set contextMap when thread start
      } else {
        MDC.clear();
      }
      //      logger.trace("dispatch cq event, wrId={}", wc.getWr_id());
      endpoint.dispatchCqEvent(wc);
    }
  }
}
