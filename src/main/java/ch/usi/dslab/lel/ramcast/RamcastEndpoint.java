package ch.usi.dslab.lel.ramcast;

import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RamcastEndpoint extends RdmaEndpoint {
    private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
    private RamcastConfig config = RamcastConfig.getInstance();
    private RamcastEndpointVerbCall verbCalls;

    // local shared memory block for receiving message from clients
    // the actual memory buffer is created at group
    // the endpoints only store register data
    // .. and shared memory block for receiving timestamp from leaders, similar to above
    private RamcastMemoryBlock sharedCircularBlock;
    private RamcastMemoryBlock sharedTimestampBlock;

    // remote shared memory block (total) and timestamp of the remote endpoint
    protected RamcastMemoryBlock remoteSharedCircularBlock;
    protected RamcastMemoryBlock remoteSharedTimeStampBlock;

    // shared buffer for client to receive server's header position
    private RamcastMemoryBlock serverHeadBlock; // this one is of client, wait to be writen
    private RamcastMemoryBlock clientBlockOfServerHead; // this one is stored on server as target to write

    // shared memory cell (segment allocated for this endpoint) of the remote endpoint
    protected RamcastMemoryBlock remoteSharedCellBlock;

    // storing node that this endpoint is connected to
    private RamcastNode node;

    // indicate if this endpoint is ready (connected, exchanged data)
    private boolean hasExchangedClientData=false;
    private boolean hasExchangedServerData=false;


    protected RamcastEndpoint(RdmaEndpointGroup<? extends RamcastEndpoint> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
        super(group, idPriv, serverSide);
    }

    @Override
    protected synchronized void init() throws IOException {
        super.init();

        // allocate and register shared buffer for client to receive server's header position
        ByteBuffer remoteHeadBuffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_REMOTE_HEAD);
        remoteHeadBuffer.putInt(0, -1);
        remoteHeadBuffer.putInt(4, -1);
        IbvMr remoteHeadMr = registerMemory(remoteHeadBuffer).execute().free().getMr();
        serverHeadBlock = new RamcastMemoryBlock(remoteHeadMr.getAddr(), remoteHeadMr.getLkey(), remoteHeadMr.getLength(), remoteHeadBuffer);

        // register the shared buffer for receiving client message.
        // the buffer was created on group
        ByteBuffer sharedCircularBuffer = ((RamcastEndpointGroup) this.group).getSharedCircularBuffer();
        IbvMr sharedCircularMr = registerMemory(sharedCircularBuffer).execute().free().getMr();
        sharedCircularBlock = new RamcastMemoryBlock(sharedCircularMr.getAddr(), sharedCircularMr.getLkey(), sharedCircularMr.getLength(), sharedCircularBuffer);

        ByteBuffer sharedTimestampBuffer = ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
        IbvMr sharedTimestampMr = registerMemory(sharedTimestampBuffer).execute().free().getMr();
        sharedTimestampBlock = new RamcastMemoryBlock(sharedTimestampMr.getAddr(), sharedTimestampMr.getLkey(), sharedTimestampMr.getLength(), sharedCircularBuffer);

        this.verbCalls = new RamcastEndpointVerbCall(this);

    }

    public void send(ByteBuffer buffer) throws IOException {
        while (!_send(buffer)) {
            try {
                Thread.sleep(0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected boolean _send(ByteBuffer buffer) throws IOException {
        logger.trace("[{}/-1] perform SEND to {}", this.endpointId, this.node);
        SVCPostSend postSend = verbCalls.freePostSend.poll();
        if (postSend != null) {
            buffer.clear();
            int index = (int) postSend.getWrMod(0).getWr_id();
            verbCalls.sendBufs[index].put(buffer);
            logger.trace("[{}/{}] sendBuff -- capacity:{} / limit:{} / remaining: {}", this.endpointId, index, verbCalls.sendBufs[index].capacity(), verbCalls.sendBufs[index].limit(), verbCalls.sendBufs[index].remaining());
            postSend.getWrMod(0).getSgeMod(0).setLength(buffer.capacity());
            postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
            postSend.getWrMod(0).setSend_flags(postSend.getWrMod(0).getSend_flags());
            verbCalls.pendingPostSend.put(index, postSend);
            postSend.execute();
            return true;
        } else {
            return false;
        }
    }


    public void dispatchCqEvent(IbvWC wc) throws IOException {
        if (wc.getStatus() == 5) {
            //flush
            return;
        } else if (wc.getStatus() != 0) {
            throw new IOException("Faulty operation! wc.status " + wc.getStatus());
        }

        if (wc.getOpcode() == 128) {
            //receiving a message
            int index = (int) wc.getWr_id();
            logger.trace("[{}/{}] deliver RECEIVE event", this.endpointId, index);
            ByteBuffer recvBuffer = verbCalls.recvBufs[index];
            dispatchReceive(recvBuffer);
            verbCalls.postRecv(index);
        } else if (wc.getOpcode() == 0) {
            //send completion
            int index = (int) wc.getWr_id();
            logger.trace("[{}/{}] deliver SEND COMPLETION event", this.endpointId, index);
            ByteBuffer sendBuffer = verbCalls.sendBufs[index];
            dispatchSendCompletion(sendBuffer);
            verbCalls.freeSend(index);
        } else {
            throw new IOException("Unkown opcode " + wc.getOpcode());
        }
    }

    private void dispatchSendCompletion(ByteBuffer buffer) {
        logger.debug("RECEIVE dispatchSendCompletion of buffer [{} {} {}]", buffer.getInt(0), buffer.getInt(4), buffer.getInt(8));
    }

    private void dispatchReceive(ByteBuffer buffer) throws IOException {
        logger.debug("SEND dispatchReceive buffer content [{} {} {}]", buffer.getInt(0), buffer.getInt(4), buffer.getInt(8));
        if (buffer.getInt(0) < 0) {
            ((RamcastEndpointGroup) this.group).handleProtocolMessage(this, buffer);
        } else {
            ((RamcastEndpointGroup) this.group).handleMessage(this, buffer);
        }
    }


    @Override
    public String toString() {
        return "{epId:" + this.getEndpointId() + "@" + this.node + "}";
    }

    public void setNode(RamcastNode node) {
        this.node = node;
    }

    public RamcastMemoryBlock getSharedCircularBlock() {
        return sharedCircularBlock;
    }

    public RamcastMemoryBlock getSharedTimestampBlock() {
        return sharedTimestampBlock;
    }

    public RamcastMemoryBlock getServerHeadBlock() {
        return serverHeadBlock;
    }

    public void setClientMemoryBlockOfRemoteHead(long remoteAddr, int remoteLKey, int remoteLength) {
        this.clientBlockOfServerHead = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
    }

    public void setRemoteSharedMemoryCellBlock(long remoteAddr, int remoteLKey, int remoteLength) {
        this.remoteSharedCellBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
    }

    public void setRemoteSharedMemoryBlock(long remoteAddr, int remoteLKey, int remoteLength) {
        this.remoteSharedCircularBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
    }

    public void setRemoteSharedTimestampMemoryBlock(long remoteAddr, int remoteLKey, int remoteLength) {
        this.remoteSharedTimeStampBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
    }

    public RamcastMemoryBlock getClientBlockOfServerHead() {
        return clientBlockOfServerHead;
    }

    public RamcastMemoryBlock getRemoteSharedCircularBlock() {
        return remoteSharedCircularBlock;
    }

    public RamcastMemoryBlock getRemoteSharedTimeStampBlock() {
        return remoteSharedTimeStampBlock;
    }

    public RamcastMemoryBlock getRemoteSharedCellBlock() {
        return remoteSharedCellBlock;
    }

    public RamcastMemoryBlock getRemoteSharedMemoryCellBlock() {
        return remoteSharedCellBlock;
    }

    public boolean isReady() {
        return hasExchangedClientData && hasExchangedServerData;
    }

    public boolean hasExchangedClientData() {
        return hasExchangedClientData;
    }

    public void setHasExchangedClientData(boolean hasExchangedClientData) {
        this.hasExchangedClientData = hasExchangedClientData;
    }

    public boolean hasExchangedServerData() {
        return hasExchangedServerData;
    }

    public void setHasExchangedServerData(boolean hasExchangedServerData) {
        this.hasExchangedServerData = hasExchangedServerData;
    }
}
