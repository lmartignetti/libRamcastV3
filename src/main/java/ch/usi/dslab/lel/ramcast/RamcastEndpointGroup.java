package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.processors.HandshakingProcessor;
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

public class RamcastEndpointGroup extends RdmaEndpointGroup<RamcastEndpoint> {

    private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
    private RamcastConfig config = RamcastConfig.getInstance();
    private RamcastAgent agent;

    private HashMap<Integer, RamcastCqProcessor<RamcastEndpoint>> cqMap;
    private int timeout;

    // shared memory for receiving message from clients
    private ByteBuffer sharedCircularBuffer;

    // shared memory for receiving timestamp from leaders
    private ByteBuffer sharedTimestampBuffer;

    private HandshakingProcessor handshakingProcessor;

    private CustomCallback customCallback;


    public RamcastEndpointGroup(RamcastAgent agent, int timeout) throws IOException {
        super(timeout);
        this.timeout = timeout;
        this.agent = agent;
        sharedCircularBuffer = allocateSharedBuffer(config.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_PACKAGE);
        sharedTimestampBuffer = allocateSharedBuffer(config.getTotalNodeCount(), config.getQueueLength(), RamcastConfig.SIZE_TIMESTAMP);

        this.cqMap = new HashMap<>();
        this.handshakingProcessor = new HandshakingProcessor(this, agent);
    }

    public static RamcastEndpointGroup createEndpointGroup(RamcastAgent agent, int timeout) throws Exception {
        RamcastEndpointGroup group = new RamcastEndpointGroup(agent, timeout);
        group.init(new RamcastEndpointFactory(group));
        return group;
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
                cqProcessor = new RamcastCqProcessor<>(context, cqSize, config.getQueueLength(), 0, 1, this.timeout, config.isPolling());
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


    public void handleProtocolMessage(RamcastEndpoint endpoint, ByteBuffer buffer) throws IOException {
        if (buffer.getInt(0) < -0 && buffer.getInt(0) > -20) { // hack: hs message has ID from -1 -> -20
            this.handshakingProcessor.handleHandshakeMessage(endpoint, buffer);
        }
    }

    public void handleReceive(RamcastEndpoint endpoint, ByteBuffer buffer) {
        if (customCallback != null) customCallback.call(buffer);
    }

    public void handleSendComplete(RamcastEndpoint endpoint, ByteBuffer buffer) {
        if (customCallback != null) customCallback.call(buffer);
    }

    public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
        this.handshakingProcessor.initHandshaking(endpoint);
    }

    public void close() throws IOException, InterruptedException {
        super.close();
        for (RamcastCqProcessor<RamcastEndpoint> cq : cqMap.values()) {
            cq.close();
        }
        logger.info("rpc group down");
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

        public RamcastCqProcessor(IbvContext context, int cqSize, int wrSize, long affinity, int clusterId,
                                  int timeout, boolean polling) throws IOException {
            super(context, cqSize, wrSize, affinity, clusterId, timeout, polling);
        }

        @Override
        public void dispatchCqEvent(RamcastEndpoint endpoint, IbvWC wc) throws IOException {
            if (contextMap != null) {
                MDC.setContextMap(contextMap);  // set contextMap when thread start
            } else {
                MDC.clear();
            }
            endpoint.dispatchCqEvent(wc);
        }
    }

    public ByteBuffer getSharedCircularBuffer() {
        return sharedCircularBuffer;
    }

    public ByteBuffer getSharedTimestampBuffer() {
        return sharedTimestampBuffer;
    }

    public void setCustomCallback(CustomCallback customCallback) {
        this.customCallback = customCallback;
    }
}

