package ch.usi.dslab.lel.ramcast.processors;

import ch.usi.dslab.lel.ramcast.*;
import com.ibm.disni.util.MemoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HandshakingProcessor {
    private static final Logger logger = LoggerFactory.getLogger(HandshakingProcessor.class);
    RamcastAgent agent;
    RamcastEndpointGroup group;

    public HandshakingProcessor(RamcastEndpointGroup group, RamcastAgent agent) {
        this.agent = agent;
        this.group = group;
    }

    public void initHandshaking(RamcastEndpoint endpoint) throws IOException {
        logger.debug("Init handshaking for endpoint {}", endpoint);
        RamcastMemoryBlock block = endpoint.getServerHeadBlock();
        ByteBuffer buffer = ByteBuffer.allocateDirect(28);
        buffer.putInt(RamcastConfig.MSG_HS_C1);
        buffer.putInt(this.agent.getNode().getGroupId());
        buffer.putInt(this.agent.getNode().getNodeId());
        buffer.putLong(block.getAddress());
        buffer.putInt(block.getLkey());
        buffer.putInt(block.getCapacity());
        logger.debug("[HS] Step 1 CLIENT Sending: group={}, node= {}, remoteHeadBlock addr={} lkey={} capacity={}", buffer.getInt(4), buffer.getInt(8), buffer.getLong(12), buffer.getInt(20), buffer.getInt(24));
        endpoint.send(buffer);
    }

    public void handleHandshakeMessage(RamcastEndpoint endpoint, ByteBuffer buffer) throws IOException {
        int ticket = buffer.getInt(0);
        if (ticket == RamcastConfig.MSG_HS_C1) {// msg step 1 sent from client
            logger.debug("[HS] Step 1 SERVER Receiving: group={}, node= {}, remoteHeadBlock addr={} lkey={} capacity={}", buffer.getInt(4), buffer.getInt(8), buffer.getLong(12), buffer.getInt(20), buffer.getInt(24));
            // store this endpoint information
            int groupId = buffer.getInt(4);
            int nodeId = buffer.getInt(8);
            RamcastNode node = RamcastNode.getNode(groupId, nodeId);
            if (node == null) {
                throw new IOException("Node not found: " + groupId + "/" + nodeId);
            }
            endpoint.setNode(node);
            // store address of the memory space to store remote head on client
            endpoint.setClientMemoryBlockOfRemoteHead(buffer.getLong(12), buffer.getInt(20), buffer.getInt(24));
            logger.debug("[HS] Step 1 SERVER setClientMemoryBlockOfRemoteHead={}", endpoint.getClientBlockOfServerHead());
            // send back to client data of the whole shared memory space
            RamcastMemoryBlock sharedCircularMemoryBlock = endpoint.getSharedCircularBlock();
            RamcastMemoryBlock sharedTimestampMemoryBlock = endpoint.getSharedTimestampBlock();
            RamcastMemoryBlock memorySegmentBlock = getNodeMemorySegmentBlock(group.getSharedCircularBuffer(), endpoint.getSharedCircularBlock().getLkey(), groupId, nodeId);
            memorySegmentBlock.setEndpoint(endpoint);

            // preparing response
            ByteBuffer response = ByteBuffer.allocateDirect(52);
            response.putInt(RamcastConfig.MSG_HS_S1); // response code

            response.putLong(sharedCircularMemoryBlock.getAddress());
            response.putInt(sharedCircularMemoryBlock.getLkey());
            response.putInt(sharedCircularMemoryBlock.getCapacity());

            response.putLong(memorySegmentBlock.getAddress());
            response.putInt(memorySegmentBlock.getLkey());
            response.putInt(memorySegmentBlock.getCapacity());

            response.putLong(sharedTimestampMemoryBlock.getAddress());
            response.putInt(sharedTimestampMemoryBlock.getLkey());
            response.putInt(sharedTimestampMemoryBlock.getCapacity());

            logger.debug("[HS] Step 1 SERVER Sending: sharedCircularMemoryBlock={} memorySegmentBlock={} sharedTimestampMemoryBlock={}", sharedCircularMemoryBlock, memorySegmentBlock, sharedTimestampMemoryBlock);

            endpoint.send(response);
            endpoint.setHasExchangedServerData(true);
            this.agent.getEndpointMap().put(node, endpoint);

            if (!endpoint.hasExchangedClientData()) this.initHandshaking(endpoint);
        } else if (ticket == RamcastConfig.MSG_HS_S1) {// msg step 1 sent from server
            logger.debug("[HS] Step 1 CLIENT Receiving: sharedBlock addr={} lkey={} capacity={} sharedSegment addr={} lkey={} capacity={}", buffer.getLong(4), buffer.getInt(12), buffer.getInt(16), buffer.getLong(20), buffer.getInt(28), buffer.getInt(32));

            endpoint.setRemoteSharedMemoryBlock(buffer.getLong(4), buffer.getInt(12), buffer.getInt(16));
            endpoint.setRemoteSharedMemoryCellBlock(buffer.getLong(20), buffer.getInt(28), buffer.getInt(32));
            endpoint.setRemoteSharedTimestampMemoryBlock(buffer.getLong(36), buffer.getInt(44), buffer.getInt(48));
            logger.debug("[HS] Step 1 CLIENT. setRemoteSharedMemoryBlock={}", endpoint.getRemoteSharedCircularBlock());
            logger.debug("[HS] Step 1 CLIENT. setRemoteSharedMemoryCellBlock={}", endpoint.getRemoteSharedMemoryCellBlock());
            endpoint.setHasExchangedClientData(true);
        } else {
            throw new IOException("Protocol msg code not found :" + ticket);
        }
    }

    public RamcastMemoryBlock getNodeMemorySegmentBlock(ByteBuffer sharedBuffer, int lkey, int groupId, int nodeId) {
        sharedBuffer.clear();
        int blockSize = RamcastConfig.getInstance().getQueueLength() * RamcastConfig.SIZE_PACKAGE;
        int pos = groupId * RamcastConfig.getInstance().getNodePerGroup() * blockSize + nodeId * blockSize;
        sharedBuffer.position(pos);
        sharedBuffer.limit(pos + blockSize);
        ByteBuffer buffer = sharedBuffer.slice();
        RamcastMemoryBlock ret = new RamcastMemoryBlock();
        ret.update(MemoryUtils.getAddress(buffer), lkey, buffer.capacity(), buffer);
        return ret;
    }

}
