package ch.usi.dslab.lel.ramcast.models;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

public class RamcastMemoryBlock {
  protected static final Logger logger = LoggerFactory.getLogger(RamcastMemoryBlock.class);
  private long address;
  private int lkey;
  private int capacity;
  private ByteBuffer buffer;
  private int headOffset;
  private int tailOffset;
  private RamcastEndpoint endpoint;

  private Set<Integer> freeableSlots;

  private boolean tailPassedHead = false;

  private RamcastMemoryBlock() {}

  public RamcastMemoryBlock(long address, int lkey, int capacity, ByteBuffer buffer) {
    this.address = address;
    this.lkey = lkey;
    this.capacity = capacity;
    this.headOffset = 0;
    this.tailOffset = 0;
    this.buffer = buffer;
    this.freeableSlots = new TreeSet<>();
  }

  //  public void update(long address, int lkey, int length, ByteBuffer buffer) {
  //    this.address = address;
  //    this.lkey = lkey;
  //    this.capacity = length;
  //    this.headOffset = 0;
  //    this.tailOffset = 0;
  //    this.buffer = buffer;
  //  }

  @Override
  public int hashCode() {
    int result = (int) (address ^ (address >>> 32));
    result = 31 * result + lkey;
    result = 31 * result + capacity;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RamcastMemoryBlock that = (RamcastMemoryBlock) o;

    if (address != that.address) return false;
    return lkey == that.lkey;
  }

  //  @Override
  //  public String toString() {
  //    return "RamcastMemoryBlock{"
  //        + "address="
  //        + address
  //        + ", lkey="
  //        + lkey
  //        + ", capacity="
  //        + capacity
  //        + '}';
  //  }

  @Override
  public String toString() {
    return "Mem{"
        + "head="
        + headOffset
        + ", tail="
        + tailOffset
        + ", address="
        + address
        + ", lkey="
        + lkey
        + ", capacity="
        + capacity
        + '}';
  }

  public RamcastEndpoint getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(RamcastEndpoint endpoint) {
    this.endpoint = endpoint;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setBuffer(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public long getAddress() {
    return address;
  }

  public int getLkey() {
    return lkey;
  }

  public int getCapacity() {
    return capacity;
  }

  public RamcastMemoryBlock copy() {
    RamcastMemoryBlock block = new RamcastMemoryBlock();
    block.address = this.address;
    block.lkey = this.lkey;
    block.capacity = this.capacity;
    block.headOffset = this.headOffset;
    block.tailOffset = this.tailOffset;
    block.buffer = this.buffer;
    block.endpoint = this.endpoint;
    return block;
  }

  public long getTail() {
    return address + tailOffset * RamcastConfig.SIZE_MESSAGE;
  }

  public void advanceTail() {
    moveTailOffset(1);
  }

  public void moveTailOffset(int slots) {
    if (tailPassedHead && tailOffset + slots > headOffset) {
      throw new IllegalStateException(
          "Tail can not pass head. Current head="
              + headOffset
              + " and tail="
              + tailOffset
              + " tail passed head="
              + tailPassedHead);
    }
    this.tailOffset += slots;
    if (tailOffset * RamcastConfig.SIZE_MESSAGE == this.capacity) {
      tailOffset = 0;
      if (!tailPassedHead) tailPassedHead = true;
    }
  }

  public void advanceHead() {
    moveHeadOffset(1);
  }

  public void moveHeadOffset(int slots) {
    if (!tailPassedHead && headOffset + slots > tailOffset) {
      throw new IllegalStateException(
          "Head can not pass tail. Current head="
              + headOffset
              + " and tail="
              + tailOffset
              + " tail passed head="
              + tailPassedHead);
    }
    this.headOffset += slots;
    if (headOffset * RamcastConfig.SIZE_MESSAGE == this.capacity) {
      headOffset = 0;
      if (tailPassedHead) tailPassedHead = false;
    }
  }

  public int getTailOffset() {
    return this.tailOffset;
  }

  public int getHeadOffset() {
    return this.headOffset;
  }

  public void setHeadOffset(int remoteHeadOffset) {}

  public int getRemainingSlots() {
    if (!tailPassedHead) {
      return (capacity / RamcastConfig.SIZE_MESSAGE) - tailOffset;
    } else {
      return headOffset - tailOffset;
    }
  }

  // free the memory slot of a message by moving the head pointer to 1 position
  public int freeSlot(int slot) {
    int freed = 0;
    if (this.headOffset == slot) {
      //      logger.debug("Freeing slot {}", slot);
      this.moveHeadOffset(1);
      freed++;
    } else {
      // this slot is in the middle of head pointer and tail pointer
      this.freeableSlots.add(slot);
    }
    // check if we can free any slot
    for (int s : freeableSlots) {
      if (this.headOffset == s) {
        //        logger.debug("Freeing slot {}", slot);
        this.moveHeadOffset(1);
        freed++;
      }
    }
    return freed;
  }

  public long getHead() {
    return address + headOffset * RamcastConfig.SIZE_MESSAGE;
  }

  public boolean isTailPassedHead() {
    return tailPassedHead;
  }
}
