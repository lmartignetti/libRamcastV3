package ch.usi.dslab.lel.ramcast.endpoint;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.models.RamcastMemoryBlock;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import ch.usi.dslab.lel.ramcast.models.RamcastNode;
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
  //  IbvMr remoteHeadMr;
  //  IbvMr sharedCircularMr;
  //  IbvMr sharedTimestampMr;

  // shared memory cell (segment allocated for this endpoint) of the remote endpoint
  private RamcastMemoryBlock remoteCellBlock;
  // shared memory cell (segment allocated for this endpoint) at local
  private RamcastMemoryBlock sharedCellBlock;
  private RamcastConfig config = RamcastConfig.getInstance();
  private RamcastEndpointVerbCall verbCalls;

  // remote shared memory block (total) and timestamp of the remote endpoint
  // the actual memory buffer is created at group
  // the endpoints only store register data
  private RamcastMemoryBlock sharedCircularBlock;
  // local shared memory block for receiving message from clients
  private RamcastMemoryBlock remoteCircularBlock;
  // .. and shared memory block for receiving timestamp from leaders, similar to above
  private RamcastMemoryBlock sharedTimestampBlock;
  private RamcastMemoryBlock remoteTimeStampBlock;

  // shared buffer for client to receive server's header position
  private RamcastMemoryBlock sharedServerHeadBlock; // this one is of client, wait to be writen
  // this one is stored on server as target to write
  private RamcastMemoryBlock remoteServerHeadBlock;
  // storing node that this endpoint is connected to
  private RamcastNode node;
  // indicate if this endpoint is ready (connected, exchanged data)
  private boolean hasExchangedClientData = false;
  private boolean hasExchangedServerData = false;
  private boolean hasExchangedPermissionData = false;

  // Memory manager for timestamp buffer, put it here to revoke later
  private IbvMr sharedTimestampMr;

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

  @Override
  protected synchronized void init() throws IOException {
    super.init();

    // allocate and register shared buffer for client to receive server's header position
    remoteHeadBuffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_REMOTE_HEAD);
    remoteHeadBuffer.putInt(0, -1);
    remoteHeadBuffer.putInt(4, -1);
    IbvMr remoteHeadMr = registerMemory(remoteHeadBuffer).execute().free().getMr();
    sharedServerHeadBlock =
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

    // extract to separated method
    //    registerTimestampWritePermission();

    //    ByteBuffer sharedTimestampBuffer =
    //        ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    //    // explicitly declare permission for this endpoint
    //    int access =
    //        IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE |
    // IbvMr.IBV_ACCESS_REMOTE_READ;
    //    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    //    sharedTimestampBlock =
    //        new RamcastMemoryBlock(
    //            sharedTimestampMr.getAddr(),
    //            sharedTimestampMr.getLkey(),
    //            sharedTimestampMr.getLength(),
    //            sharedTimestampBuffer);

    verbCalls = new RamcastEndpointVerbCall(this);
  }

  public RamcastMemoryBlock registerTimestampReadPermission() throws IOException {
    // if the buffer has been register -> deregister it
    if (this.sharedTimestampMr != null) this.sharedTimestampMr.deregMr().execute().free();

    ByteBuffer sharedTimestampBuffer =
        ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    sharedTimestampBlock =
        new RamcastMemoryBlock(
            sharedTimestampMr.getAddr(),
            sharedTimestampMr.getLkey(),
            sharedTimestampMr.getLength(),
            sharedTimestampBuffer);
    if (RamcastConfig.LOG_ENABLED)
      logger.info(
          "Shared timestamp block with READ permission for {}: {}",
          this.node,
          sharedTimestampBlock);
    return sharedTimestampBlock;
  }

  public RamcastMemoryBlock registerTimestampWritePermission() throws IOException {
    // if the buffer has been register -> deregister it
    if (this.sharedTimestampMr != null) this.sharedTimestampMr.deregMr().execute().free();
    ByteBuffer sharedTimestampBuffer =
        ((RamcastEndpointGroup) this.group).getSharedTimestampBuffer();
    // explicitly declare permission for this endpoint
    int access =
        IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
    sharedTimestampMr = getPd().regMr(sharedTimestampBuffer, access).execute().free().getMr();
    sharedTimestampBlock =
        new RamcastMemoryBlock(
            sharedTimestampMr.getAddr(),
            sharedTimestampMr.getLkey(),
            sharedTimestampMr.getLength(),
            sharedTimestampBuffer);
    if (RamcastConfig.LOG_ENABLED)
      logger.info(
          "Shared timestamp block with READ/WRITE permission for {}: {}",
          this.node,
          sharedTimestampBlock);
    return sharedTimestampBlock;
  }

  public void dispatchCqEvent(IbvWC wc) throws IOException {
    if (wc.getStatus() == 5) {
      // flush
      return;
    } else if (wc.getStatus() == 10) { // permission error //IBV_WC_REM_ACCESS_ERR
      // happens when permission to write to this memory has been revoke
      // or just simply incorrect registration in the boostrap
      int index = (int) wc.getWr_id();
      dispatchPermissionError(verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset]);
      return;
    } else if (wc.getStatus() != 0) {
      throw new IOException("Faulty operation! wc.status " + wc.getStatus());
    }

    if (wc.getOpcode() == 128) {
      // receiving a message
      int index = (int) wc.getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/{}] deliver RECEIVE event", this.endpointId, index);
      ByteBuffer recvBuffer = verbCalls.recvBufs[index];
      dispatchReceive(recvBuffer);
      verbCalls.postRecv(index);
    } else if (wc.getOpcode() == 0) {
      // send completion
      int index = (int) wc.getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/{}] deliver SEND COMPLETION event", this.endpointId, index);
      ByteBuffer sendBuffer = verbCalls.sendBufs[index];
      dispatchSendCompletion(sendBuffer);
      verbCalls.freeSend(index);
    } else if (wc.getOpcode() == 1) { // IBV_WC_RDMA_WRITE SIGNAL BACK
      // write completion
      int index = (int) wc.getWr_id();
      if (index >= verbCalls.updatePostIndexOffset) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace("[{}/{}] deliver WRITE SIGNAL COMPLETION event", this.endpointId, index);
        verbCalls.freeUpdate(index);
      } else {
        ByteBuffer buffer = verbCalls.writeBufs[index];
        if (RamcastConfig.LOG_ENABLED)
          logger.trace("[{}/{}] deliver WRITE MESSAGE COMPLETION event", this.endpointId, index);
        verbCalls.freeWrite(index);
        dispatchWriteCompletion(buffer);
      }
    } else {
      throw new IOException("Unkown opcode " + wc.getOpcode());
    }
  }

  protected boolean _send(ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace("[{}/-1] perform SEND to {}", this.endpointId, this.node);
    SVCPostSend postSend = verbCalls.freeSendPostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer sendBuf = verbCalls.sendBufs[index];
      sendBuf.clear();
      sendBuf.put(buffer);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
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
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  public int putToBuffer(ByteBuffer buffer, Class type, Object value) {
    //    logger.trace("Wring to buffer {} class {} value {}", buffer, type, value);
    if (type == Integer.TYPE) {
      buffer.putInt((Integer) value);
      return Integer.BYTES;
    } else if (type == Long.TYPE) {
      buffer.putLong((Long) value);
      return Long.BYTES;
    } else if (type == Character.TYPE) {
      buffer.putChar((Character) value);
      return Character.BYTES;
    } else if (type == Short.TYPE) {
      buffer.putShort((Short) value);
      return Short.BYTES;
    } else if (type == Double.TYPE) {
      buffer.putDouble((Double) value);
      return Double.BYTES;
    } else if (type == Float.TYPE) {
      buffer.putFloat((Float) value);
      return Float.BYTES;
    }
    return 0;
  }

  // data should be in the form of a pair of <type> <value>: Integer 4 long 2
  protected boolean _writeSignal(long address, int lkey, Object... data) throws IOException {
    SVCPostSend postSend = verbCalls.freeUpdatePostSend.poll();
    boolean signaled = unsignaledUpdateCount % config.getSignalInterval() == 0;
    if (postSend != null) {
      int index = (int) postSend.getWrMod(0).getWr_id();
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[{}/{}] perform WRITE SIGNAL {} to {} at address {} lkey {} values {}",
            this.endpointId,
            index,
            signaled ? "SIGNALED" : "UNSIGNALED",
            this.node,
            address,
            lkey,
            data);

      ByteBuffer updateBuffer = verbCalls.updateBufs[index - verbCalls.updatePostIndexOffset];
      updateBuffer.clear();
      int length = 0;
      for (int i = 0; i < data.length; i++) {
        length += putToBuffer(updateBuffer, (Class) data[i], data[++i]);
      }
      if (length > RamcastConfig.SIZE_SIGNAL) {
        throw new IOException(
            "Buffer size of [" + length + "] is too big. Only allow " + RamcastConfig.SIZE_SIGNAL);
      }
      postSend.getWrMod(0).getSgeMod(0).setLength(length);
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(address);
      postSend.getWrMod(0).getRdmaMod().setRkey(lkey);
      if (signaled) {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED | IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount = 1;
        verbCalls.pendingUpdatePostSend.put(index, postSend);
        postSend.execute();
      } else {
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_INLINE);
        unsignaledUpdateCount++;
        postSend.execute();
        verbCalls.freeUpdatePostSend.add(postSend);
      }
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  protected boolean _writeMessage(ByteBuffer buffer, boolean signaled) throws IOException {
    //    logger.trace("[{}/-1] perform WRITE on SHARED BUFFER of {}", this.endpointId, this.node);
    if (buffer.capacity() > RamcastConfig.SIZE_PAYLOAD) {
      throw new IOException(
          "Buffer size of ["
              + buffer.capacity()
              + "] is too big. Only allow "
              + RamcastConfig.SIZE_PAYLOAD);
    }

    int availableSlots = this.getAvailableSlots();
    if (availableSlots <= 0) {
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[{}/-1] Don't have available slot for writing message. {}",
            this.endpointId,
            availableSlots);
      return false;
    }
    SVCPostSend postSend = verbCalls.freeWritePostSend.poll();
    if (postSend != null) {
      buffer.clear();
      int index = (int) postSend.getWrMod(0).getWr_id();
      ByteBuffer writeBuf = verbCalls.writeBufs[index];
      writeBuf.clear();
      writeBuf.put(buffer);
      writeBuf.putLong(
          RamcastConfig.POS_CHECKSUM, StringUtils.calculateCrc32((ByteBuffer) buffer.clear()));
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[{}/{}] WRITE at position {} buffer capacity:{} / limit:{} / remaining: {} / first int {} / checksum {} / tail {}",
            this.endpointId,
            index,
            this.remoteCellBlock.getTailOffset(),
            writeBuf.capacity(),
            writeBuf.limit(),
            writeBuf.remaining(),
            writeBuf.getInt(0),
            writeBuf.getLong(RamcastConfig.POS_CHECKSUM),
            this.remoteCellBlock.getTailOffset());
      postSend.getWrMod(0).getSgeMod(0).setLength(RamcastConfig.SIZE_MESSAGE);
      postSend.getWrMod(0).getSgeMod(0).setAddr(MemoryUtils.getAddress(writeBuf));
      postSend.getWrMod(0).getRdmaMod().setRemote_addr(this.remoteCellBlock.getTail());
      this.remoteCellBlock.advanceTail();
      postSend.getWrMod(0).getRdmaMod().setRkey(this.remoteCellBlock.getLkey());
      if (signaled) {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(
              "[{}/{}] client Signaled remote write. Adding postSend to pending list. Remote mem {}",
              this.endpointId,
              index,
              this.remoteCellBlock);
        postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        verbCalls.pendingWritePostSend.put(index, postSend);
      } else {
        postSend.getWrMod(0).setSend_flags(0);
      }
      postSend.execute();
      if (!signaled) {
        if (RamcastConfig.LOG_ENABLED)
          logger.debug(
              "[{}/{}] client Unsignaled remote write. Adding back postSend to available list. Remote mem {}",
              this.endpointId,
              index,
              this.remoteCellBlock);
        verbCalls.freeWritePostSend.add(postSend);
        dispatchUnsignaledWriteCompletion(buffer);
      }
      return true;
    } else {
      if (RamcastConfig.LOG_ENABLED)
        logger.trace("[{}/-1] don't have available postsend. Wait", this.endpointId);
      return false;
    }
  }

  /*private int getAvailableSlotOnRemote() {
    int freed = this.getRemoteHead();
    if (freed > 0) this.remoteSharedCellBlock.moveHeadOffset(freed);
    return this.remoteSharedCellBlock.getRemainingSlots();
  }*/

  private void dispatchPermissionError(ByteBuffer buffer) {
    ((RamcastEndpointGroup) this.group).handlePermissionError(buffer);
  }

  private void dispatchRemoteWrite(RamcastMessage message) throws IOException {
    ((RamcastEndpointGroup) this.group).handleReceiveMessage(message);
  }

  private void dispatchWriteCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
          "dispatch WriteCompletion of buffer [{} {} {} / crc {}]",
          buffer.getInt(0),
          buffer.getInt(4),
          buffer.getInt(8),
          buffer.getLong(RamcastConfig.POS_CHECKSUM));
  }

  private void dispatchUnsignaledWriteCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
          "dispatch UnsignaledWriteCompletion of buffer [{} {} {}]",
          buffer.getInt(0),
          buffer.getInt(4),
          buffer.getInt(8));
  }

  private void dispatchSendCompletion(ByteBuffer buffer) {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
          "dispatch SendCompletion of buffer [{} {} {}]",
          buffer.getInt(0),
          buffer.getInt(4),
          buffer.getInt(8));
    ((RamcastEndpointGroup) this.group).handleSendComplete(this, buffer);
  }

  private void dispatchReceive(ByteBuffer buffer) throws IOException {
    if (RamcastConfig.LOG_ENABLED)
      logger.trace(
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

  public int getAvailableSlots() {
    int freed = this.remoteHeadBuffer.getInt(0);
    if (freed > 0) {
      this.remoteHeadBuffer.putInt(0, 0);
      this.remoteCellBlock.moveHeadOffset(freed);
      if (RamcastConfig.LOG_ENABLED)
        logger.debug(
            "[{}/] CLIENT do SET HEAD of {} to move {} positions, memory: {}",
            this.endpointId,
            this.node,
            freed,
            this.remoteCellBlock);
    }
    return this.remoteCellBlock.getRemainingSlots();
  }

  @Override
  public String toString() {
    return "{epId:" + this.getEndpointId() + "@" + this.node + "}";
  }

  public RamcastMemoryBlock getSharedCellBlock() {
    return sharedCellBlock;
  }

  public void setSharedCellBlock(RamcastMemoryBlock sharedCellBlock) {
    this.sharedCellBlock = sharedCellBlock;
  }

  public RamcastMemoryBlock getSharedCircularBlock() {
    return sharedCircularBlock;
  }

  public RamcastMemoryBlock getSharedTimestampBlock() {
    return sharedTimestampBlock;
  }

  public RamcastMemoryBlock getSharedServerHeadBlock() {
    return sharedServerHeadBlock;
  }

  public void setClientMemoryBlockOfRemoteHead(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteServerHeadBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedMemoryCellBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteCellBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedMemoryBlock(long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteCircularBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public void setRemoteSharedTimestampMemoryBlock(
      long remoteAddr, int remoteLKey, int remoteLength) {
    this.remoteTimeStampBlock = new RamcastMemoryBlock(remoteAddr, remoteLKey, remoteLength, null);
  }

  public RamcastMemoryBlock getRemoteServerHeadBlock() {
    return remoteServerHeadBlock;
  }

  public RamcastMemoryBlock getRemoteCircularBlock() {
    return remoteCircularBlock;
  }

  public RamcastMemoryBlock getRemoteTimeStampBlock() {
    return remoteTimeStampBlock;
  }

  public RamcastMemoryBlock getRemoteCellBlock() {
    return remoteCellBlock;
  }

  public RamcastMemoryBlock getRemoteSharedMemoryCellBlock() {
    return remoteCellBlock;
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

  public boolean hasExchangedPermissionData() {
    // if the remote side of this endpoint is not leader, and this is not from a leader node =>
    // do'nt need to exchange data
    if (!this.getNode().isLeader() && !((RamcastEndpointGroup) this.group).getAgent().isLeader())
      return true;
    // or this is leader && remote side is leader => this connect to itself
    if (this.getNode().equals(((RamcastEndpointGroup) group).getAgent().getNode())) return true;
    return hasExchangedPermissionData;
  }

  public void setHasExchangedPermissionData(boolean hasExchangedPermissionData) {
    this.hasExchangedPermissionData = hasExchangedPermissionData;
  }

  @Override
  public synchronized void close() throws IOException, InterruptedException {
    super.close();
    verbCalls.close();
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

  public void writeSignal(long address, int lkey, Object... values) throws IOException {
    while (!_writeSignal(address, lkey, values)) {
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

  public void pollForData() {
    ByteBuffer recvBuffer = this.sharedCellBlock.getBuffer();
    try {
      recvBuffer.clear();
      int start = this.sharedCellBlock.getTailOffset() * RamcastConfig.SIZE_MESSAGE;
      recvBuffer.position(start);

      long crc = recvBuffer.getLong(start + RamcastConfig.POS_CHECKSUM);
      if (crc == 0) {
        return;
      }

      recvBuffer.position(start);
      recvBuffer.limit(start + RamcastConfig.SIZE_PAYLOAD);

      long checksum = StringUtils.calculateCrc32(recvBuffer);

      if (checksum != crc) {
        if (RamcastConfig.LOG_ENABLED)
          logger.trace(
              "Message is not completed. Calculated CRC {} vs Read CRC {}. Buffer [{} {} {}]",
              checksum,
              crc,
              recvBuffer.getInt(start),
              recvBuffer.getInt(start + 4),
              recvBuffer.getInt(start + 8));
        return;
      }
      if (RamcastConfig.LOG_ENABLED)
        logger.trace(
            "[{}] dispatch local recvBuffer at position {}, buffer [{} {} {}], CRC {}",
            this.endpointId,
            this.sharedCellBlock.getTailOffset(),
            recvBuffer.getInt(start),
            recvBuffer.getInt(start + 4),
            recvBuffer.getInt(start + 8),
            checksum);
      RamcastMemoryBlock clone = this.sharedCellBlock.copy();

      RamcastMessage message =
          new RamcastMessage(
              ((ByteBuffer) recvBuffer.position(start).limit(start + RamcastConfig.SIZE_PAYLOAD))
                  .slice(),
              this.sharedCellBlock);

      // reset recvBuffer
      recvBuffer.clear();
      recvBuffer.putLong(start + RamcastConfig.POS_CHECKSUM, 0);
      // update remote head
      this.sharedCellBlock.advanceTail();
      if (RamcastConfig.LOG_ENABLED)
        logger.debug("[{}] SERVER MEMORY after polling: {}", this.endpointId, this.sharedCellBlock);

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

  public RamcastNode getNode() {
    return node;
  }

  public void setNode(RamcastNode node) {
    this.node = node;
  }
}
