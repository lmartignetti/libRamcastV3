package ch.usi.dslab.lel.ramcast;

import com.ibm.disni.RdmaServerEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RamcastAgent {
    private static final Logger logger = LoggerFactory.getLogger(RamcastAgent.class);
    RamcastConfig config = RamcastConfig.getInstance();
    private Map<RamcastNode, RamcastEndpoint> endpointMap;


    private RamcastEndpointGroup endpointGroup;

    private RamcastNode node;
    private RdmaServerEndpoint<RamcastEndpoint> serverEp;

    public RamcastAgent(int groupId, int nodeId) throws Exception {
        MDC.put("ROLE", groupId + "/" + nodeId);
        this.node = RamcastNode.getNode(groupId, nodeId);
        this.endpointMap = new ConcurrentHashMap<>();
    }

    private void connect(RamcastNode node) {
        try {
            RamcastEndpoint endpoint = endpointGroup.createEndpoint();
            endpoint.connect(node.getInetAddress(), config.getTimeout());
            endpoint.setNode(node);
            endpointGroup.initHandshaking(endpoint);
            while (!endpoint.isReady()) Thread.sleep(10);
            endpointMap.put(node, endpoint);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void establishConnections() throws Exception {
        // listen for new connections
        this.endpointGroup = RamcastEndpointGroup.createEndpointGroup(this, config.getTimeout());
        this.serverEp = endpointGroup.createServerEndpoint();
        this.serverEp.bind(this.node.getInetAddress(), 100);
        Thread listener = new Thread(() -> {
            MDC.put("ROLE", node.getGroupId() + "/" + node.getNodeId());
            while (true) {
                try {
                    RamcastEndpoint endpoint = this.serverEp.accept();
                    while (!endpoint.isReady()) Thread.sleep(10);
                    logger.debug(">>> Server accept connection of endpoint {}. CONNECTION READY", endpoint);
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                    logger.error("Error accepting connection", e);
                }
            }
        });
        listener.setName("ConnectionListener");
        listener.start();

        // trying to establish a bi-direction connection
        // A node only connect to node with bigger ids.
        for (RamcastNode node : RamcastGroup.getAllNodes()) {
            if (node.getOrderId() > this.node.getOrderId()) {
                logger.debug("connecting to: {}", node);
                this.connect(node);
                logger.debug(">>> Client connected to: {}. CONNECTION READY", node);
            }
        }

        while (true) {
            Thread.sleep(10);
            if (this.endpointMap.keySet().size() != config.getTotalNodeCount() - 1) continue;
            if (this.endpointMap.values().stream().map(RamcastEndpoint::isReady).reduce(Boolean::logicalAnd).get())
                break;
        }

        logger.debug("Agent of node {} is ready. EndpointMap: Key:{} Vlue {}", this.node, this.endpointMap.keySet(), this.endpointMap.values());
    }

    public Map<RamcastNode, RamcastEndpoint> getEndpointMap() {
        return endpointMap;
    }

    public void send(int groupId, int nodeId, ByteBuffer buffer) throws IOException {
        send(RamcastNode.getNode(groupId, nodeId), buffer);
    }

    public void send(RamcastNode node, ByteBuffer buffer) throws IOException {
        this.endpointMap.get(node).send(buffer);
    }

    public RamcastNode getNode() {
        return node;
    }

    public RamcastEndpointGroup getEndpointGroup() {
        return endpointGroup;
    }

    @Override
    public String toString() {
        return "RamcastAgent{" +
                "node=" + node +
                '}';
    }

    public void close() throws IOException, InterruptedException {

        this.serverEp.close();
        for (RamcastEndpoint endpoint : endpointMap.values()) {
            endpoint.close();
        }
        this.endpointGroup.close();
    }
}
