package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.RamcastAgent;
import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.*;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RamcastEndpointGroup extends RdmaEndpointGroup<RamcastEndpoint> {

  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpointGroup.class);
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastAgent agent;

  private HashMap<Integer, RamcastCqProcessor<RamcastEndpoint>> cqMap;
  private int timeout;

  // for leader election
  private AtomicInteger currentBallotNumber;
  private AtomicInteger sequenceNumber; // for sending
  private AtomicInteger currentSequenceNumber; // for checking msg when receive
  private AtomicInteger localClock; // for clock for ordering msg

  // storing memory segment block associated with each endpoint
  //  private Map<RamcastEndpoint, RamcastMemoryBlock> endpointMemorySegmentMap;

  // storing all endponints of all nodes
  private Map<RamcastNode, RamcastEndpoint> nodeEndpointMap;
  private Map<Integer, RamcastEndpoint> incomingEndpointMap;
  private List<RamcastEndpoint> incomingEndpoints;
  private Map<Integer, List<RamcastEndpoint>> groupEndpointsMap;

  // shared memory for receiving message from clients
  private ByteBuffer sharedCircularBuffer;

  // shared memory for receiving timestamp from leaders
  private ByteBuffer sharedTimestampBuffer;
  private RamcastTsMemoryBlock timestampBlock;

  private HandshakingProcessor handshakingProcessor;
  private LeaderElectionProcessor leaderElectionProcessor;
  private MessageProcessor messageProcessor;

  private CustomHandler customHandler;

  public RamcastEndpointGroup(RamcastAgent agent, int timeout) throws IOException {
    super(timeout);
    this.timeout = timeout;
    this.agent = agent;
    this.sharedCircularBuffer =
            allocateSharedBuffer(
                    RamcastGroup.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_MESSAGE);
    this.sharedTimestampBuffer =
            allocateShareTimestampdBuffer(
                    RamcastGroup.getTotalNodeCount(),
                    config.getQueueLength(),
                    RamcastConfig.SIZE_TIMESTAMP);
    this.timestampBlock =
            new RamcastTsMemoryBlock(0, 0, sharedTimestampBuffer.capacity(), sharedTimestampBuffer);
    this.nodeEndpointMap = new ConcurrentHashMap<>();
    this.incomingEndpointMap = new ConcurrentHashMap<>();
    this.groupEndpointsMap = new ConcurrentHashMap<>();
    this.cqMap = new HashMap<>();
    this.handshakingProcessor = new HandshakingProcessor(this, agent);
    this.leaderElectionProcessor = new LeaderElectionProcessor(this, agent);
    this.messageProcessor = new MessageProcessor(this, agent);
    this.currentBallotNumber = new AtomicInteger(0);
    this.sequenceNumber = new AtomicInteger(0);
    this.currentSequenceNumber = new AtomicInteger(0);
    this.incomingEndpoints = new ArrayList<>(0);
    this.localClock = new AtomicInteger(0);
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
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("Write message to {}, buffer {}, ep {}", node, buffer, this.nodeEndpointMap);
    this.nodeEndpointMap.get(node).writeMessage(buffer);
  }

  public void writeMessage(RamcastGroup group, ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "Write message to {}, buffer {}, ep {}",
              group,
              buffer,
              this.groupEndpointsMap.get(group.getId()));
    for (RamcastEndpoint endpoint : this.getGroupEndpointsMap().get(group.getId())) {
      endpoint.writeMessage(buffer);
    }
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

  public void handleReceiveMessage(RamcastMessage message) throws IOException {
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
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(">>> Client exchanged permission data to: {}.", endpoint.getNode());
    }
    // wait for receiving acks from all nodes
    while (this.leaderElectionProcessor.getAcks().get()
            // todo: find nicer way for -1
            != RamcastGroup.getTotalNodeCount()) Thread.sleep(10);
    if (RamcastConfig.LOG_ENABLED)
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
                      if (RamcastConfig.LOG_ENABLED) logger.info("Polling for incoming data");
                      while (true) {
                        for (int i = 0; i < incomingEndpoints.size(); i++) {
                          if (incomingEndpoints.get(i) != null) {
                            incomingEndpoints.get(i).pollForData();
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
    //    int freed = memoryBlock.freeSlot(message.getSlotOfGroupId(agent.getGroupId()));
    int freed =
            memoryBlock.freeSlot(message.getGroupSlot(message.getGroupIndex(agent.getGroupId())));
    RamcastEndpoint endpoint = message.getMemoryBlock().getEndpoint();
    // free ts
    // TODO: enable this
    timestampBlock.freeTimestamp(message);

    if (freed == 0) {
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
                "[{}] Can't release memory slot. There are pending slots", endpoint.getEndpointId());
      return;
    }
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
              "[{}] SERVER MEMORY after releasing memory: {}",
              endpoint.getEndpointId(),
              endpoint.getSharedCellBlock());

    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[{}] Released memory of {} slot. Update client [{}].", message.getId(), freed, endpoint);
    message.reset();
    this.writeRemoteHeadOnClient(endpoint, freed, message.getId());
  }

  // a node writes its timestamp to ts mem of remote node, at its index
  public void writeTimestamp(
          RamcastMessage message, int ballotNumber, int sequenceNumber, int localClock)
          throws IOException {
    // for each group in the destination
    int thisGroupIndex = message.getGroupIndex(this.agent.getGroupId());
    for (int groupIndex = 0; groupIndex < message.getGroupCount(); groupIndex++) {
      // target groupId
      int groupId = message.getGroup(groupIndex);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
                "[{}] SERVER PUT TS of group [{}] to memory of group [{}] with value [{}/{}] groupEndpointsMap={}",
                message.getId(),
                agent.getGroupId(),
                groupId,
                ballotNumber,
                sequenceNumber,
                groupEndpointsMap);
      // for each endpoint of the destination group
      for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
        if (endpoint == null) continue; // endpoint could be null due to failed process
        // only update other leaders + processes of this node's own group
        if (endpoint.getNode().isLeader() || endpoint.getGroupId() == agent.getGroupId()) {
          if (endpoint
                  .getNode()
                  .equals(agent.getNode())) { // if the node is writing to itself -> do local
            if (RamcastConfig.LOG_ENABLED)
              logger.debug(
                      "[{}] group [{}] updates ts value [{}/{}] for its local buffer on {} at index {} epi {}",
                      message.getId(),
                      agent.getGroupId(),
                      ballotNumber,
                      sequenceNumber,
                      endpoint.getNode(),
                      thisGroupIndex,
                      endpoint.getEndpointId());
            timestampBlock.writeLocalTs(
                    message, thisGroupIndex, ballotNumber, sequenceNumber, localClock);
          } else {
            if (RamcastConfig.LOG_ENABLED)
              logger.debug(
                      "[{}] group [{}] updates ts value [{}/{}] on {} at index {} epi {}",
                      message.getId(),
                      agent.getGroupId(),
                      ballotNumber,
                      sequenceNumber,
                      endpoint.getNode(),
                      thisGroupIndex,
                      endpoint.getEndpointId());
            writeTimestamp(
                    endpoint, message, thisGroupIndex, ballotNumber, sequenceNumber, localClock);
          }
        }
      }
    }
  }

  public void writeTimestamp(
          RamcastEndpoint endpoint,
          RamcastMessage message,
          int groupIndex,
          int ballotNumber,
          int sequenceNumber,
          int value)
          throws IOException {
    RamcastTsMemoryBlock timestampBlock = endpoint.getRemoteTimeStampBlock();
    long address = timestampBlock.getNodeTimestampAddress(message, groupIndex);
    endpoint.writeSignal(
            address,
            timestampBlock.getLkey(),
            Integer.TYPE,
            ballotNumber,
            Integer.TYPE,
            sequenceNumber,
            Integer.TYPE,
            value);
  }

  // for unit test
  public void writeTimestamp(
          RamcastEndpoint endpoint,
          int slot,
          int groupIndex,
          int ballotNumber,
          int sequenceNumber,
          int value)
          throws IOException {
    RamcastTsMemoryBlock timestampBlock = endpoint.getRemoteTimeStampBlock();
    long address = timestampBlock.getNodeTimestampAddress(slot, endpoint.getNode(), groupIndex);
    endpoint.writeSignal(
            address,
            timestampBlock.getLkey(),
            Integer.TYPE,
            ballotNumber,
            Integer.TYPE,
            sequenceNumber,
            Integer.TYPE,
            value);
  }

  public void writeRemoteHeadOnClient(RamcastEndpoint endpoint, int headOffset, int msgId)
          throws IOException {
    RamcastMemoryBlock clientBlock = endpoint.getRemoteServerHeadBlock();
    endpoint.writeSignal(
            clientBlock.getAddress(),
            clientBlock.getLkey(),
            Integer.TYPE,
            headOffset,
            Integer.TYPE,
            msgId);
  }

  public void leaderPropageTs(
          RamcastMessage message, int ballotNumber, int clock, int groupId, int groupIndex)
          throws IOException {
    int newSeqNumber = this.sequenceNumber.incrementAndGet();
    if (RamcastConfig.LOG_ENABLED) logger.debug(
            "[{}] leader of group {} propagating timestamp [{}/{}/{}] of group {} index {} ",
            message.getId(),
            agent.getGroupId(),
            ballotNumber,
            newSeqNumber,
            clock,
            groupId,
            groupIndex);

    for (RamcastEndpoint endpoint : groupEndpointsMap.get(agent.getGroupId())) {
      if (endpoint == null) continue;
      if (endpoint.getNode() == agent.getNode()) {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(
                  "[{}] group [{}] propagate ts value [{}/{}/{}] for its local buffer on {} at index {} epi {}",
                  message.getId(),
                  agent.getGroupId(),
                  ballotNumber,
                  newSeqNumber,
                  clock,
                  endpoint.getNode(),
                  groupIndex,
                  endpoint.getEndpointId());
        timestampBlock.writeLocalTs(message, groupIndex, ballotNumber, newSeqNumber, clock);
      } else {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(
                  "[{}] group [{}] propagate ts value [{}/{}/{}] on {} at index {} epi {}",
                  message.getId(),
                  agent.getGroupId(),
                  ballotNumber,
                  newSeqNumber,
                  clock,
                  endpoint.getNode(),
                  groupIndex,
                  endpoint.getEndpointId());
        writeTimestamp(endpoint, message, groupIndex, ballotNumber, newSeqNumber, clock);
      }
    }
  }

  public void sendAck(
          RamcastMessage message, int ballotNumber, int sequenceNumber, int groupId, int groupIndex)
          throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[{}] {} sending acks for timestamp [{}/{}] of group {} index {}",
              message.getId(),
              agent.getNode(),
              ballotNumber,
              sequenceNumber,
              groupId,
              groupIndex);
    for (int i = 0; i < message.getGroupCount(); i++) {
      for (RamcastEndpoint endpoint : groupEndpointsMap.get((int) message.getGroup(i))) {
        if (endpoint == null) continue;
        if (endpoint.getNode() == agent.getNode()) {
          if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "[{}] {} sending acks for timestamp [{}/{}] of group {} index {} to local buffer {}",
                    message.getId(),
                    agent.getNode(),
                    ballotNumber,
                    sequenceNumber,
                    groupId,
                    groupIndex,
                    endpoint.getNode());
          if (RamcastConfig.LOG_ENABLED)
            logger.debug("[{}] message before writing local ack {}", message.getId(), message);
          message.writeAck(endpoint.getNode(), ballotNumber, sequenceNumber);
          if (RamcastConfig.LOG_ENABLED)
            logger.debug("[{}] message after writing local ack {}", message.getId(), message);
        } else {
          if (RamcastConfig.LOG_ENABLED)
            logger.debug(
                    "[{}] {} sending acks for timestamp [{}/{}] of group {} index {} to remote memory {}",
                    message.getId(),
                    agent.getNode(),
                    ballotNumber,
                    sequenceNumber,
                    groupId,
                    groupIndex,
                    endpoint.getNode());
          writeAck(endpoint, message, ballotNumber, sequenceNumber);
        }
      }
    }
  }

  private void writeAck(
          RamcastEndpoint endpoint, RamcastMessage message, int ballotNumber, int sequenceNumber)
          throws IOException {
    int offset = message.getPosAck(agent.getNode());

    // need to get memory segment in the shared memory of remote node that store msg of that node
    int blockSize = RamcastConfig.getInstance().getQueueLength() * RamcastConfig.SIZE_MESSAGE;
    int pos =
            message.getSource().getGroupId() * RamcastConfig.getInstance().getNodePerGroup() * blockSize
                    + message.getSource().getNodeId() * blockSize;
    long address =
            endpoint.getRemoteCircularBlock().getAddress()
                    + pos
                    + message.getSlot() * RamcastConfig.SIZE_MESSAGE
                    + offset
                    + RamcastConfig
                    .SIZE_BUFFER_LENGTH; // IMPORTANT: because msg is shifted 4 bytes to the right =>
    // need to add 4 bytes here

    if (RamcastConfig.LOG_ENABLED)
      logger.debug(
              "[{}] write ack to {} pos in shared segment: {}, base address {}, absolute addr {}, offset of this ack {}, absolute add of offset {}",
              message.getId(),
              endpoint.getNode(),
              pos,
              endpoint.getRemoteCircularBlock().getAddress(),
              pos + endpoint.getRemoteCircularBlock().getAddress(),
              offset,
              address);

    endpoint.writeSignal(
            address,
            endpoint.getRemoteCellBlock().getLkey(),
            Integer.TYPE,
            ballotNumber,
            Integer.TYPE,
            sequenceNumber);
  }

  public boolean allEndpointReady(int groupId, int msgId) {
    for (RamcastEndpoint endpoint : groupEndpointsMap.get(groupId)) {
      if (endpoint == null) continue;
      if (endpoint.getCompletionSignal() != -1 && endpoint.getCompletionSignal() != msgId) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
                  "[{}] endpoint of group {} is not ready for message {} Completion signal:{}",
                  endpoint.getEndpointId(),
                  groupId,
                  msgId,
                  endpoint.getCompletionSignal());
        return false;
      }
    }
    return true;
  }

  public void readTimestampMemBlock(RamcastEndpoint endpoint) {
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

  public Map<Integer, RamcastEndpoint> getIncomingEndpointMap() {
    return incomingEndpointMap;
  }

  public Map<RamcastNode, RamcastEndpoint> getNodeEndpointMap() {
    return nodeEndpointMap;
  }

  public AtomicInteger getBallotNumber() {
    return currentBallotNumber;
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

  private ByteBuffer allocateShareTimestampdBuffer(int connectionCount, int queueLength, int size) {
    int capacity =
            connectionCount
                    * (queueLength * RamcastConfig.getInstance().getGroupCount() * size
                    + RamcastConfig.SIZE_FUO);
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
    if (RamcastConfig.LOG_ENABLED) logger.trace("setting up cq processor");
    IbvContext context = endpoint.getIdPriv().getVerbs();
    if (context != null) {
      if (RamcastConfig.LOG_ENABLED) logger.trace("setting up cq processor, context found");
      RamcastCqProcessor<RamcastEndpoint> cqProcessor;
      int key = context.getCmd_fd();
      if (!cqMap.containsKey(key)) {
        int cqSize = (config.getQueueLength() * 2) * 3 * RamcastGroup.getTotalNodeCount();
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
    if (RamcastConfig.LOG_ENABLED) logger.trace("registering endpoint with cq");
    cqProcessor.registerQP(qp.getQp_num(), endpoint);
    return qp;
  }

  @Override
  public void allocateResources(RamcastEndpoint endpoint) throws Exception {
    endpoint.allocateResources();
  }

  public void disconnect(RamcastEndpoint endpoint) throws IOException, InterruptedException {
    RamcastNode node = endpoint.getNode();
    RamcastGroup group = RamcastGroup.getGroup(endpoint.getGroupId());
    nodeEndpointMap.remove(node);
    incomingEndpointMap.remove(endpoint.getEndpointId());
    incomingEndpoints.remove(endpoint);
    groupEndpointsMap.get(endpoint.getGroupId()).remove(endpoint);
    group.removeNode(node);
    endpoint.close();
    logger.info("endpoint {} disconnect", endpoint);

    if (node.isLeader() && agent.shouldBeLeader()) {
      this.beLeader();
    }
  }

  private void beLeader() throws IOException, InterruptedException {
    logger.info("THIS NODE {} WILL BE LEADER", agent.getNode());
    requestWritePermission();
    logger.info("Start syncing data");
    logger.debug("Pending messages: {}", messageProcessor.getProcessing());
    logger.debug("Timestamps: {}", timestampBlock);
    syncState();
  }

  private void syncState() throws IOException, InterruptedException {
    Map<RamcastEndpoint, RamcastTsMemoryBlock> tsBlocks = new HashMap<>();
    for (RamcastEndpoint endpoint : groupEndpointsMap.get(agent.getGroupId())) {
      RamcastTsMemoryBlock tsBlock = endpoint.readTimestampMemorySpace();
      logger.debug(
              "read ts block cap {} from endpoint {} {}. the last position {}:[{}/{}/{}/{}]",
              tsBlock.getCapacity(),
              endpoint,
              tsBlock,
              tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1,
              tsBlock.readSlotBallot(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
              tsBlock.readSlotSequence(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
              tsBlock.readSlotValue(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1),
              tsBlock.readSlotStatus(tsBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP - 1));
      tsBlocks.put(endpoint, tsBlock);
    }

    for (int i = 0; i < timestampBlock.getCapacity() / RamcastConfig.SIZE_TIMESTAMP; i++) {
      int thisBallot = timestampBlock.readSlotBallot(i);
      int thisSequence = timestampBlock.readSlotSequence(i);
      int thisValue = timestampBlock.readSlotValue(i);
      byte thisStatus = timestampBlock.readSlotStatus(i);
      for (Map.Entry<RamcastEndpoint, RamcastTsMemoryBlock> entry : tsBlocks.entrySet()) {
        RamcastEndpoint endpoint = entry.getKey();
        RamcastTsMemoryBlock tsMemoryBlock = entry.getValue();
        int thatBallot = tsMemoryBlock.readSlotBallot(i);
        int thatSequence = tsMemoryBlock.readSlotSequence(i);
        int thatValue = tsMemoryBlock.readSlotValue(i);
        byte thatStatus = tsMemoryBlock.readSlotStatus(i);

//        if (thisBallot )
      }
    }
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
    for (RamcastNode node : RamcastGroup.getAllNodes()) {
      if (node.getOrderId() >= agent.getNode().getOrderId()) {
        if (RamcastConfig.LOG_ENABLED) logger.debug("connecting to: {}", node);
        RamcastEndpoint endpoint = createEndpoint();
        endpoint.connect(node.getInetAddress(), config.getTimeout());
        endpoint.setNode(node);
        // if the node is connecting to itself
        if (node.equals(agent.getNode())) {
          getNodeEndpointMap().put(node, endpoint);
          List<RamcastEndpoint> eps =
                  getGroupEndpointsMap().computeIfAbsent(node.getGroupId(), k -> new ArrayList<>());
          eps.add(endpoint);
        }
        initHandshaking(endpoint);
        while (!endpoint.isReady()) Thread.sleep(10);
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(">>> Client connected to: {}. CONNECTION READY", endpoint);
      }
    }
  }

  public void setBallotNumber(int ballotNumnber) {
    currentBallotNumber.set(ballotNumnber);
  }

  public AtomicInteger getSequenceNumber() {
    return sequenceNumber;
  }

  public RamcastTsMemoryBlock getTimestampBlock() {
    return timestampBlock;
  }

  public AtomicInteger getCurrentSequenceNumber() {
    return currentSequenceNumber;
  }

  public MessageProcessor getMessageProcessor() {
    return messageProcessor;
  }

  public void revokeTimestampWritePermission() throws IOException {
    for (RamcastEndpoint ramcastEndpoint : getNodeEndpointMap().values()) {
      // only revoke permission of nodes in same group
      // and are not leader
      if (ramcastEndpoint.getNode().getGroupId() == agent.getNode().getGroupId()
              && !ramcastEndpoint.getNode().isLeader()) {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(
                  "Revoking write permission of {} on {}", ramcastEndpoint.getNode(), agent.getNode());
        ramcastEndpoint.registerTimestampReadPermission();
      }
    }
  }

  public List<RamcastEndpoint> getIncomingEndpoints() {
    return incomingEndpoints;
  }

  public AtomicInteger getLocalClock() {
    return localClock;
  }

  public void updateTsStatus(RamcastMessage message) {
    timestampBlock.setDelivered(message);
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
      if (RamcastConfig.LOG_ENABLED) {
        if (contextMap != null) {
          MDC.setContextMap(contextMap); // set contextMap when thread start
        } else {
          MDC.clear();
        }
      }
      endpoint.dispatchCqEvent(wc);
    }
  }
}
