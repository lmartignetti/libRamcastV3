package ch.usi.dslab.lel.ramcast.models;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.utils.StringUtils;

import java.nio.ByteBuffer;

public class RamcastMessage {
  private static final int POS_ID = 0;
  private static final int POS_MSG_LENGTH = RamcastConfig.SIZE_MSG_ID;
  private static final int POS_MSG = RamcastConfig.SIZE_MSG_ID + RamcastConfig.SIZE_MSG_LENGTH;

  private static RamcastConfig config = RamcastConfig.getInstance();
  // the memory block where this message is located
  private RamcastMemoryBlock memoryBlock;
  private int id = -1;
  private int messageLength;

  // the buffer of whole message
  private ByteBuffer buffer;
  // the buffer has payload
  private ByteBuffer message;

  // number of destination groups
  private short groupsCount;
  // number of id of groups in the destinagtion
  private short[] groups;
  // the offset of this message in the shared block
  private short slot;
  // this offset of this message in each group.
  // In the same group, this should be same for all nodes
  private short[] slots;
  // the absolute address of that offset. should not in the buffer
  private long address;

  // storing acks of other nodes
  private short[][] groupsAcks;
  private short[][] groupsAckBallots;

  public RamcastMessage(ByteBuffer message, int[] groups) {
    this.message = message;
    this.message.clear();
    this.messageLength = message.capacity();

    this.groups = new short[groups.length];
    this.slots = new short[groups.length];
    for (short i = 0; i < groups.length; i++) {
      this.groups[i] = (short) groups[i];
      this.slots[i] = 0;
    }
    this.groupsCount = (short) groups.length;
    this.buffer = null;
    this.address = -1;
    this.groupsAcks = new short[groups.length][config.getFollowerCount()];
    this.groupsAckBallots = new short[groups.length][config.getFollowerCount()];
  }

  public RamcastMessage(ByteBuffer buffer, RamcastMemoryBlock memoryBlock) {
    this.buffer = buffer;
    this.memoryBlock = memoryBlock;
    this.slot = (short) memoryBlock.getTailOffset();
    this.address = memoryBlock.getTail();
  }

  public ByteBuffer toBuffer() {
    ByteBuffer ret = ByteBuffer.allocateDirect(RamcastConfig.SIZE_PAYLOAD);
    this.message.clear();
    ret.putInt(this.id);
    ret.putInt(this.messageLength);
    ret.put(this.message);
    ret.putShort(this.groupsCount);
    for (short i = 0; i < this.groupsCount; i++) {
      ret.putShort(this.groups[i]);
    }
    for (int i = 0; i < this.groupsCount; i++) {
      ret.putLong(this.slots[i]);
    }
    return ret;
  }

  public int getMessageLength() {
    if (this.messageLength <= 0) {
      this.messageLength = this.buffer.getInt(POS_MSG_LENGTH);
    }
    return this.messageLength;
  }

  private int getPosMeta() {
    return POS_MSG + this.getMessageLength();
  }

  private int getPosGroupsCount() {
    return getPosMeta();
  }

  private int getPosGroups() {
    return getPosGroupsCount() + RamcastConfig.SIZE_MSG_GROUP_COUNT;
  }

  private int getPosSlots() {
    return getPosGroups() + RamcastConfig.SIZE_MSG_GROUP * getGroupCount();
  }

  private int getPosAcks() {
    return getPosSlots() * RamcastConfig.SIZE_MSG_SLOT * getGroupCount();
  }

  public short getGroupCount() {
    if (this.groupsCount <= 0) {
      this.groupsCount = this.buffer.getShort(getPosGroupsCount());
    }
    return this.groupsCount;
  }

  public ByteBuffer getMessage() {
    if (this.message == null) {
      this.message =
          ((ByteBuffer) this.buffer.position(POS_MSG).limit(POS_MSG + this.getMessageLength()))
              .slice();
      this.message.clear();
      this.buffer.clear();
    }
    return this.message;
  }

  public short getGroup(int index) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groups[index];

    if (this.groups == null) {
      this.groups = new short[this.getGroupCount()];
    }
    if (this.groups[index] <= 0) {
      this.groups[index] = this.buffer.getShort(getPosGroups() + 2 * index);
    }
    return this.groups[index];
  }

  private short getGroupSlot(int index) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.slots[index];

    if (this.slots == null) {
      this.slots = new short[this.getGroupCount()];
    }
    if (this.slots[index] <= 0) {
      this.slots[index] =
          this.buffer.getShort(getPosSlots() + RamcastConfig.SIZE_MSG_OFFSET * index);
    }
    return this.slots[index];
  }

  public short getAck(int groupIndex, int nodeIndex) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groupsAcks[groupIndex][nodeIndex];

    if (this.groupsAcks == null) {
      this.groupsAcks = new short[this.getGroupCount()][config.getFollowerCount()];
    }
    if (this.groupsAcks[groupIndex][nodeIndex] <= 0) {
      int index = groupIndex * config.getFollowerCount() + nodeIndex;
      int pos = this.getPosAcks() + RamcastConfig.SIZE_ACK * index;
      try {
        this.groupsAcks[groupIndex][nodeIndex] = this.buffer.getShort(pos);
      } catch (Exception e) {
        // there is a case buffer has not been completed.
        return -1;
      }
    }
    return this.groupsAcks[groupIndex][nodeIndex];
  }

  public short getAckBallot(int groupIndex, int nodeIndex) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groupsAckBallots[groupIndex][nodeIndex];

    if (this.groupsAckBallots == null) {
      this.groupsAckBallots = new short[this.getGroupCount()][config.getFollowerCount()];
    }
    if (this.groupsAckBallots[groupIndex][nodeIndex] <= 0) {
      int index = groupIndex * config.getFollowerCount() + nodeIndex;
      int pos = this.getPosAcks() + RamcastConfig.SIZE_ACK * index;
      try {
        this.groupsAckBallots[groupIndex][nodeIndex] =
            this.buffer.getShort(pos + RamcastConfig.SIZE_ACK_VALUE);
      } catch (Exception e) {
        // there is a case buffer has not been completed.
        return -1;
      }
    }
    return this.groupsAckBallots[groupIndex][nodeIndex];
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    ret.append(this.getId())
        .append("║l=")
        .append(this.getMessageLength())
        .append("║")
        .append(this.getMessage().getInt(0))
        .append("..msg║gs=")
        .append(this.getGroupCount())
        .append("║");
    StringBuilder dests = new StringBuilder("g=");
    StringBuilder offset = new StringBuilder("o=");
    StringBuilder acks = new StringBuilder("ak=");
    for (int i = 0; i < this.getGroupCount(); i++) dests.append(this.getGroup(i)).append("│");
    for (int i = 0; i < this.getGroupCount(); i++) offset.append(this.getGroupSlot(i)).append("│");

    for (int i = 0; i < this.getGroupCount(); i++) {
      for (int j = 0; j < config.getFollowerCount(); j++)
        acks.append(this.getAck(i, j)).append("/").append(this.getAckBallot(i, j)).append("|");
      acks = new StringBuilder(acks.substring(0, acks.length() - 1));
      acks.append("│");
    }
    dests = new StringBuilder(dests.substring(0, dests.length() - 1));
    offset = new StringBuilder(offset.substring(0, offset.length() - 1));
    acks = new StringBuilder(acks.substring(0, acks.length() - 1));
    ret.append(dests).append("║").append(offset).append("║").append(acks);
    return StringUtils.formatMessage(ret.toString());
  }

  public void setId(int id) {
    this.id = id;
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setSlots(short[] slots) {
    this.slots = slots;
  }

  public int getSlot() {
    return slot;
  }

  public long getAddress() {
    return address;
  }

  public RamcastMemoryBlock getMemoryBlock() {
    return memoryBlock;
  }

  public int getId() {
    if (this.id < 0) {
      this.id = ((ByteBuffer) this.buffer.clear()).getInt(POS_ID);
    }
    return id;
  }

  public void reset() {}
}
