package ch.usi.dslab.lel.ramcast;

import java.nio.ByteBuffer;

public class RamcastMessage {
  private static RamcastConfig config = RamcastConfig.getInstance();

  // the memory block where this message is located
  private RamcastMemoryBlock memoryBlock;
  private int id;
  private int messageLength;
  private ByteBuffer buffer;
  // number of destination groups
  private short groupCount;
  // number of id of groups in the destinagtion
  private short[] groups;
  // the offset of this message in the shared block
  private int addressOffset;
  // the absolute address of that offset
  private long address;

  public RamcastMessage(ByteBuffer buffer, RamcastMemoryBlock memoryBlock) {
    this.buffer = buffer;
    this.memoryBlock = memoryBlock;
    this.addressOffset = memoryBlock.getTailOffset();
    this.address = memoryBlock.getTail();
  }

  public RamcastMessage(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public int getAddressOffset() {
    return addressOffset;
  }

  public long getAddress() {
    return address;
  }

  public RamcastMemoryBlock getMemoryBlock() {
    return memoryBlock;
  }
}
