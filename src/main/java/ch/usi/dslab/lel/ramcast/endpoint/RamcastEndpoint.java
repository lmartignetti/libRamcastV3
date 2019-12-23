package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.RamcastMessage;
import ch.usi.dslab.lel.ramcast.RamcastNode;
import ch.usi.dslab.lel.ramcast.utils.StringUtils;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RamcastEndpoint extends RdmaEndpoint {
  private static final Logger logger = LoggerFactory.getLogger(RamcastEndpoint.class);
  // remote shared memory block (total) and timestamp of the remote endpoint
  protected RamcastMemoryBlock remoteSharedCircularBlock;
  protected RamcastMemoryBlock remoteSharedTimeStampBlock;
  // shared memory cell (segment allocated for this endpoint) of the remote endpoint
  protected RamcastMemoryBlock remoteSharedCellBlock;
  IbvMr remoteHeadMr;
  IbvMr sharedCircularMr;
  IbvMr sharedTimestampMr;
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastEndpointVerbCall verbCalls;
  // local shared memory block for receiving message from clients
  // the actual memory buffer is created at group
  // the endpoints only store register data
  // .. and shared memory block for receiving timestamp from leaders, similar to above
  private RamcastMemoryBlock sharedCircularBlock;
  private RamcastMemoryBlock sharedTimestampBlock;
  // shared buffer for client to receive server's header position
  private RamcastMemoryBlock serverHeadBlock; // this one is of client, wait to be writen
  private RamcastMemoryBlock
      clientBlockOfServerHead; // this one is stored on server as target to write
  // storing node that this endpoint is connected to
  private RamcastNode node;
  // indicate if this endpoint is ready (connected, exchanged data)
  private boolean hasExchangedClientData = false;
  private boolean hasExchangedServerData = false;

  private int unsignaledWriteCount = 0;
  private int unsignaledUpdateCount = 0;

  private ByteBuffer
      remoteHeadBuffer; // shared buffer for client to receive server's header position. need to be
  // here to not being clean

  protected RamcastEndpoint(
      RdmaEndpointGroup<? extends RamcastEndpoint> group, RdmaCmId idPriv, boolean serverSide)
      throws IOException {
    super(group, idPriv, serverSide);
  }

  protected boolean _send(ByteBuffer buffer) throws IOException {
    logger.trace("[{}/-1] perform SEND to {}", this.endpointId, this.node);
    SVCPostSend postSend = verbCalls.freeSendPostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer sendBuf = verbCalls.sendBufs[index];
      sendBuf.clear();
      sendBuf.put(buffer);
      logger.trace(
          "[{}/{}] sendBuff -- capacity:{} / limit:{} / remaining: {} / first Int {}",
          this.endpointId,
          index,
          sendBuf.capacity(),
          sendBuf.limit(),
          sendBuf.remaining(),
          sendBuf.getInt(0));
      postSend.getWrMod(0).getSgeMod(0).setLength(buffer.capacity());
      postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
      postSend.getWrMod(0).setSend_flags(postSend.getWrMod(0).getSend_flags());
      verbCalls.pendingSendPostSend.put(index, postSend);
      postSend.execute();
      return true;
    } else {
      return false;
    }
  }

  protected boolean _writeSignal(ByteBuffer buffer, long address, int lkey, int length)
      throws IOException {
    logger.trace(
        "[{}/-1] perform WRITE SIGNAL to {} at address {} lkey {} length {}",
        this.endpointId,
        this.node,
        address,
        lkey,
        length);
    if (buffer.capacity() > RamcastConfig.SIZE_SIGNAL || length > RamcastConfig.SIZE_SIGNAL) {
      throw new IOException(
          "Buffer size of ["
              + buffer.capacity()
              + "] is too big. Only allow "
              + RamcastConfig.SIZE_SIGNAL);
    }
    SVCPostSend postSend = verbCalls.freeUpdatePostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer updateBuffer = verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset];
      updateBuffer.clear();
      updateBuffer.put(buffer);
      postSend.getWrMod(0).getSgeMod(0).setLength(length);
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
      postSend.getWrMod(0).getRdmaMod().setRkey(lkey);
      if (unsignaledUpdateCount % config.getSignalInterval() == 0) {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount = 1;
      } else {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount++;
      }
      postSend.execute();
      verbCalls.freeUpdatePostSend.add(postSend);
      return true;
    } else {
      return false;
    }
  }

  protected boolean _writeMessage(ByteBuffer buffer, boolean signaled) throws IOException {
    logger.trace("[{}/-1] perform WRITE on SHARED BUFFER of {}", this.endpointId, this.node);
    if (buffer.capacity() > RamcastConfig.SIZE_PAYLOAD) {
      throw new IOException(
          "Buffer size of ["
              + buffer.capacity()
              + "] is too big. Only allow "
              + RamcastConfig.SIZE_PAYLOAD);
    }
    SVCPostSend postSend = verbCalls.freeWritePostSend.poll();
    int availableSlots = this.remoteSharedCellBlock.getRemainingSlots();
    if (availableSlots <= 0) {
      logger.debug(
          "[{}/-1] Don't have available slot for writing message. {}",
          this.endpointId,
          availableSlots);
      verbCalls.freeWritePostSend.add(postSend);
      return false;
    }
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer writeBuf = verbCalls.writeBufs[index];
      writeBuf.clear();
      writeBuf.put(buffer);
      writeBuf.putLong(
          RamcastConfig.POS_CHECKSUM, StringUtils.calculateCrc32((ByteBuffer) buffer.clear()));
      logger.trace(
          "[{}/{}] WRITE at position {} buffer capacity:{} / limit:{} / remaining: {} / first int {} / checksum {}",
          this.endpointId,
          index,
          this.remoteSharedCellBlock.getTailOffset(),
          writeBuf.capacity(),
          writeBuf.limit(),
          writeBuf.remaining(),
          writeBuf.getInt(0),
          writeBuf.getLong(RamcastConfig.POS_CHECKSUM));
      postSend.getWrMod(0).getSgeMod(0).setLength(RamcastConfig.SIZE_MESSAGE);
      postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(writeBuf));
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(this.remoteSharedCellBlock.getTail());
      this.remoteSharedCellBlock.moveTailOffset(1);
      postSend.getWrMod(0).getRdmaMod().setRkey(this.remoteSharedCellBlock.getLkey());
      if (signaled) {
        logger.trace(
            "[{}/{}] client Signaled remote write. Adding postSend to pending list",
            this.endpointId,
            index);
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        verbCalls.pendingWritePostSend.put(index, postSend);
      }
      postSend.execute();
      if (!signaled) {
        logger.trace(
            "[{}/{}] client Unsignaled remote write. Adding back postSend to available list",
            this.endpointId,
            index);
        verbCalls.freeWritePostSend.add(postSend);
        dispatchUnsignaledWriteCompletion(buffer);
      }
      return true;
    } else {
      return false;
    }
  }

  public void dispatchCqEvent(IbvWC wc) throws IOException {
    if (wc.getStatus() == 5) {
      // flush
      return;
    } else if (wc.getStatus() != 0) {
      throw new IOException("Faulty operation! wc.status " + wc.getStatus());
    }

    if (wc.getOpcode() == 128) {
      // receiving a message
      int index = (int) wc.getWr_id();
      logger.trace("[{}/{}] deliver RECEIVE event", this.endpointId, index);
      ByteBuffer recvBuffer = verbCalls.recvBufs[index];
      dispatchReceive(recvBuffer);
      verbCalls.postRecv(index);
    } else if (wc.getOpcode() == 0) {
      // send completion
      int index = (int) wc.getWr_id();
      logger.trace("[{}/{}] deliver SEND COMPLETION event", this.endpointId, index);
      ByteBuffer sendBuffer = verbCalls.sendBufs[index];
      dispatchSendCompletion(sendBuffer);
      verbCalls.freeSend(index);
    } else if (wc.getOpcode() == 1) { // IBV_WC_RDMA_WRITE SIGNAL BACK
      // write completion
      int index = (int) wc.getWr_id();
      if (index >= verbCalls.updatePostIndexOffset) {
        logger.trace("[{}/{}] deliver WRITE SIGNAL COMPLETION event", this.endpointId, index);
      } else {
        ByteBuffer buffer = verbCalls.writeBufs[index];
        logger.trace("[{}/{}] deliver WRITE MESSAGE COMPLETION event", this.endpointId, index);
        dispatchWriteCompletion(buffer);
        verbCalls.freeWrite(index);
      }
    } else {
      throw new IOException("Unkown opcode " + wc.getOpcode());
    }
  }

  private void dispatchRemoteWrite(RamcastMessage message) {
    ((RamcastEndpointGroup) this.group).handleReceiveMessage(message);
  }

  private void dispatchWriteCompletion(ByteBuffer buffer) {
    logger.debug(
        "dispatch WriteCompletion of buffer [{} {} {} / crc {}]",
        buffer.getInt(0),
        buffer.getInt(4),
        buffer.getInt(8),
        buffer.getLong(RamcastConfig.POS_CHECKSUM));
  }

  private void dispatchUnsignaledWriteCompletion(ByteBuffer buffer) {
    logger.debug(
        "dispatch UnsignaledWriteCompletion of buffer [{} {} {}]",
        buffer.getInt(0),
        buffer.getInt(4),
        buffer.getInt(8));
  }

  private void dispatchSendCompletion(ByteBuffer buffer) {
    logger.debug(
        "dispatch SendCompletion of buffer [{} {} {}]",
        buffer.getInt(0),
        buffer.getInt(4),
        buffer.getInt(8));
    ((RamcastEndpointGroup) this.group).handleSendComplete(this, buffer);
  }

  private void dispatchReceive(ByteBuffer buffer) throws IOException {
    logger.debug(
        "dispatch Receive of buffer [{} {} {}]",
        buffer.getInt(0),
        buffer.getInt(4),
        buffer.getInt(8));
    if (buffer.getInt(0) < 0) {
      ((RamcastEndpointGroup) this.group).handleProtocolMessage(this, buffer);
    } else {
      ((RamcastEndpointGroup) this.group).handleReceive(this, buffer);
    }
  }

  public int getRemoteHead() {
    int remoteHeadOffset = this.remoteHeadBuffer.getInt(0);
    if (remoteHeadOffset != -1 && remoteHeadOffset != this.remoteSharedCellBlock.getHeadOffset()) {
      this.remoteSharedCellBlock.moveHeadOffset(remoteHeadOffset);
      logger.trace(
          "[{}/] CLIENT do SET HEAD of {} to move {} positions, head {} tail {}",
          this.endpointId,
          this.node,
          remoteHeadOffset,
          this.serverHeadBlock.getHeadOffset(),
          this.serverHeadBlock.getTailOffset());
      return remoteHeadOffset;
    }
    return -1;
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
    this.clientBlockOfServerHead =
        new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedMemoryCellBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteSharedCellBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedMemoryBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteSharedCircularBlock =
        new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedTimestampMemoryBlock(
      long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteSharedTimeStampBlock =
        new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
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

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    super.close();
    verbCalls.close();
  }

  @Override
  protected synchronized void init() throws IOException {
    super.init();

    // allocate and register shared buffer for client to receive server's header position
    remoteHeadBuffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_REMOTE_HEAD);
    remoteHeadBuffer.putInt(0, -1);
    remoteHeadBuffer.putInt(4, -1);
    IbvMr remoteHeadMr = registerMemory(remoteHeadBuffer).execute().free().getMr();
    serverHeadBlock =
        new RamcastMemoryBlock(
            remoteHeadMr.getAddr(),
            remoteHeadMr.getLkey(),
            remoteHeadMr.getLength(),
            remoteHeadBuffer);

    // register the shared buffer for receiving client message.
    // the buffer was created on group
    ByteBuffer sharedCircularBuffer = ((RamcastEndpointGroup) this.group).getSharedCircularBuffer();
    IbvMr sharedCircularMr = registerMemory(sharedCircularBuffer).execute().free().getMr();
    sharedCircularBlock =
        new RamcastMemoryBlock(
            sharedCircularMr.getAddr(),
            sharedCircularMr.getLkey(),
            sharedCircularMr.getLength(),
            sharedCircularBuffer);

    ByteBuffer sharedTimestampBuffer =
        ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    IbvMr sharedTimestampMr = registerMemory(sharedTimestampBuffer).execute().free().getMr();
    sharedTimestampBlock =
        new RamcastMemoryBlock(
            sharedTimestampMr.getAddr(),
            sharedTimestampMr.getLkey(),
            sharedTimestampMr.getLength(),
            sharedCircularBuffer);

    verbCalls = new RamcastEndpointVerbCall(this);
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

  public void writeSignal(ByteBuffer buffer, long address, int lkey, int length)
      throws IOException {
    while (!_writeSignal(buffer, address, lkey, length)) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void writeMessage(ByteBuffer buffer) throws IOException {
    boolean signaled = unsignaledWriteCount % config.getSignalInterval() == 0;
    if (signaled) {
      unsignaledWriteCount = 1;
    } else {
      unsignaledWriteCount += 1;
    }
    while (!_writeMessage(buffer, signaled)) {
      try {
        Thread.sleep(0);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  public void pollForData(RamcastMemoryBlock block) {
    ByteBuffer recvBuffer = block.getBuffer();
    try {
      recvBuffer.clear();
      int start = block.getTailOffset() * RamcastConfig.SIZE_MESSAGE;
      recvBuffer.position(start);

      long crc = recvBuffer.getLong(start + RamcastConfig.POS_CHECKSUM);
      if (crc == 0) {
        return;
      }

      recvBuffer.position(start);
      recvBuffer.limit(start + RamcastConfig.SIZE_PAYLOAD);

      long checksum = StringUtils.calculateCrc32(recvBuffer);

      if (checksum != crc) {
        logger.trace(
            "Message is not completed. Calculated CRC {} vs Read CRC {}. Buffer [{} {} {}]",
            checksum,
            crc,
            recvBuffer.getInt(start),
            recvBuffer.getInt(start + 4),
            recvBuffer.getInt(start + 8));
        return;
      }
      logger.trace(
          "[{}] dispatch local recvBuffer at position {}", this.endpointId, block.getTailOffset());
      RamcastMemoryBlock clone = block.copy();
      RamcastMessage message =
          new RamcastMessage(
              (ByteBuffer) recvBuffer.position(start).limit(start + RamcastConfig.SIZE_PAYLOAD),
              block);

      // reset recvBuffer
      recvBuffer.clear();
      recvBuffer.putLong(start + RamcastConfig.POS_CHECKSUM, 0);

      // update remote head
      block.moveTailOffset(1);
      logger.trace(
          "[{}] SERVER MEMORY after polling: HEAD={} TAIL={}",
          this.endpointId,
          block.getHeadOffset(),
          block.getTailOffset());

      this.dispatchRemoteWrite(message);
    } catch (Exception e) {
      //            System.out.println("Ticket=" + ticket + " LENGTH=" + length + "
      // buffer.position=" + recvBuffer.position() + " buffer.limit=" + recvBuffer.limit() + "
      // buffer.remaining=" + recvBuffer.remaining() + " reading at " + (block.getHeadOffset() +
      // length - 4));
      e.printStackTrace();
      logger.error("Error pollForData", e);
      System.exit(1);
    }
  }
}
