package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.*;
import ch.usi.dslab.lel.ramcast.processors.HandshakingProcessor;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RamcastEndpointGroup extends RdmaEndpointGroup<RamcastEndpoint> {

  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastAgent agent;

  private HashMap<Integer, RamcastCqProcessor<RamcastEndpoint>> cqMap;
  private int timeout;

  // storing memory segment block associated with each endpoint
  private Map<RamcastEndpoint, RamcastMemoryBlock> endpointMemorySegmentMap;

  // storing all endponints of all nodes
  private Map<RamcastNode, RamcastEndpoint> endpointMap;

  // shared memory for receiving message from clients
  private ByteBuffer sharedCircularBuffer;

  // shared memory for receiving timestamp from leaders
  private ByteBuffer sharedTimestampBuffer;

  private HandshakingProcessor handshakingProcessor;
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

    this.endpointMap = new ConcurrentHashMap<>();
    this.cqMap = new HashMap<>();
    this.handshakingProcessor = new HandshakingProcessor(this, agent);
    this.messageProcessor = new MessageProcessor(this, agent);
    this.endpointMemorySegmentMap = new ConcurrentHashMap<>();
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
    this.endpointMap.get(node).send(buffer);
  }

  public void writeMessage(RamcastNode node, ByteBuffer buffer) throws IOException {
    this.endpointMap.get(node).writeMessage(buffer);
  }

  public void handleProtocolMessage(RamcastEndpoint endpoint, ByteBuffer buffer)
      throws IOException {
    if (buffer.getInt(0) < -0 && buffer.getInt(0) > -20) { // hack: hs message has ID from -1 -> -20
      this.handshakingProcessor.handleHandshakeMessage(endpoint, buffer);
    }
  }

  public void handleReceive(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleReceive(buffer);
  }

  public void handleSendComplete(RamcastEndpoint endpoint, ByteBuffer buffer) {
    if (customHandler != null) customHandler.handleSendComplete(buffer);
  }

  public void handleReceiveMessage(RamcastMessage message) {
    if (customHandler != null) customHandler.handleReceiveMessage(message.getBuffer());
    this.messageProcessor.handleMessage(message);
  }

  public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
    this.handshakingProcessor.initHandshaking(endpoint);
  }

  public void startPollingData() {
    Thread serverDataPolling =
        new Thread(
            () -> {
              logger.info("Polling for incoming data");
              while (true) {
                for (Map.Entry<RamcastEndpoint, RamcastMemoryBlock> e :
                    endpointMemorySegmentMap.entrySet()) {
                  RamcastEndpoint endpoint = e.getKey();
                  RamcastMemoryBlock block = endpointMemorySegmentMap.get(endpoint);
                  if (endpoint != null) {
                    endpoint.pollForData(block);
                  }
                }
                Thread.yield();
              }
            });
    serverDataPolling.setName("ServerDataPolling");
    serverDataPolling.start();
  }

//  public void releaseMemory(RamcastMessage message) throws IOException, InterruptedException {
//    RamcastMemoryBlock memoryBlock = message.getMemoryBlock();
//    memoryBlock.freeSlot(message.getAddressOffset());
//    length += RamcastEndpoint.headerSize;
//    long end = start + length;
//
//      logger.trace("[{}]Trying to release memory space from {} to {}, length={}, block head {} tail {} cap {}, endpoint {}", message.getId(), start, end, length, memoryBlock.getHeadOffset(), memoryBlock.getTailOffset(), memoryBlock.getCapacity(), memoryBlock.getEndpoint().getEndpointId());
//    memoryBlock.freeSegment(start, end);
//
//      logger.debug("[{}] SERVER MEMORY after freeSegment: HEAD={} TAIL={} CAP={}", message.getId(), memoryBlock.getHeadOffset(), memoryBlock.getTailOffset(), memoryBlock.getCapacity());
//    message.resetReserved();
//
//    while (!memoryBlock.getEndpoint().update(message.getId(), memoryBlock.getEndpoint().getRemoteHeadMemory().getAddress(), memoryBlock.getEndpoint().getRemoteHeadMemory().getLkey(), memoryBlock.getHeadOffset(), message.getId(), 0, 8))
//      Thread.sleep(0);
//  }

  public void updateRemoteHeadOnClient(RamcastEndpoint endpoint, int headOffset)
      throws IOException {
    ByteBuffer buffer = ByteBuffer.allocateDirect(4);
    buffer.putInt(headOffset);
    RamcastMemoryBlock clientBlock = endpoint.getClientBlockOfServerHead();
    endpoint.writeSignal(
        buffer, clientBlock.getAddress(), clientBlock.getLkey(), buffer.capacity());
  }

  public void close() throws IOException, InterruptedException {
    super.close();
    for (RamcastEndpoint endpoint : endpointMap.values()) {
      endpoint.close();
    }
    for (RamcastCqProcessor<RamcastEndpoint> cq : cqMap.values()) {
      cq.close();
    }
    logger.info("rpc group down");
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

  public Map<RamcastEndpoint, RamcastMemoryBlock> getEndpointMemorySegmentMap() {
    return endpointMemorySegmentMap;
  }

  public Map<RamcastNode, RamcastEndpoint> getEndpointMap() {
    return endpointMap;
  }

  private ByteBuffer allocateSharedBuffer(int connectionCount, int queueLength, int packageSize) {
    int capacity = connectionCount * queueLength * packageSize;
    return ByteBuffer.allocateDirect(capacity);
  }

  protected synchronized IbvQP createQP(RdmaCmId id, IbvPd pd, IbvCQ cq) throws IOException {
    IbvQPInitAttr attr = new IbvQPInitAttr();
    attr.cap().setMax_recv_wr(config.getQueueLength());
    attr.cap().setMax_send_wr(config.getQueueLength());
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
      endpoint.dispatchCqEvent(wc);
    }
  }
}
