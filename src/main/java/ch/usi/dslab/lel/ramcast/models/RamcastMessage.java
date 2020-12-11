package ch.usi.dslab.lel.ramcast.models;

import ch.usi.dslab.lel.ramcast.RamcastConfig;
import ch.usi.dslab.lel.ramcast.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class RamcastMessage {

  /*

  0        4        8              20            22            26                    30         38           46          54          62          70
  ╠════════╬════════╬══════════════╬═════════════╬═════════════╬═════════════════════╬═══════════╬═══════════════════════════════════════════════╣
  ║   4    ║   4    ║     12       ║    2        ║    2 * 2    ║     2 * 2           ║    8      ║                  8 * r * n                    ║
  ╠════════╬════════╬══════════════╬═════════════╬═════════════╬═════════════════════╬═══════════╬═══════════════════════════════════════════════╣
  ║  id    ║ length ║  message(m)  ║ g count     ║ g[0] │ g[1] ║ slot[0]  │ slot[1]  ║    crc    ║ ack[0][0] │ ack[0][1] │ ack[1][0] │ ack[1][1] ║
  ╚════════╩════════╩══════════════╩═════════════╩═════════════╩═════════════════════╩═══════════╩═══════════════════════════════════════════════╝

  0        4        8              20            22        24        26           34         42            50
  ╠════════╬════════╬══════════════╬═════════════╬═════════╬══════==══╬═══════════╬════════════════════════╣
  ║   4    ║   4    ║     12       ║    2        ║    2    ║    2     ║    8      ║       8 * r * n        ║
  ╠════════╬════════╬══════════════╬═════════════╬═════════╬══════════╬═══════════╬════════════════════════╣
  ║  id    ║ length ║  message(m)  ║ g count     ║   g[0]  ║ slot[0]  ║    crc    ║ ack[0][0] │ ack[0][1]  ║
  ╚════════╩════════╩══════════════╩═════════════╩═════════╩══════════╩═══════════╩════════════════════════╝


  */

  // id: int: 4
  // length: int: 4
  // groupCount: short: 2
  // message: this.messageLength
  // groups: short[]: 2* groupCount
  // acks: short[]: 2* groupCount

  public static final int POS_ID = 0;
  public static final int POS_MSG_LENGTH = RamcastConfig.SIZE_MSG_ID;
  public static final int POS_MSG = RamcastConfig.SIZE_MSG_ID + RamcastConfig.SIZE_MSG_LENGTH;
  private static final Logger logger = LoggerFactory.getLogger(RamcastMessage.class);
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
  private int[][] groupsAcksBallots;
  private int[][] groupsAckSequences;

  private Map<Short, Short> groupsMap;

  private RamcastNode source; // from which node this message has been sent

  private int finalTs;

  private ByteBuffer serializedBuffer;

  // this is processed by the leader, should not in the buffer
  private int pendingTimestampUpdates = 0; // used by leader
  private int completedTimestampUpdates = 0; // used by leader


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
    this.groupsAcksBallots = new int[groups.length][config.getNodePerGroup()];
    this.groupsAckSequences = new int[groups.length][config.getNodePerGroup()];
  }

  public RamcastMessage(ByteBuffer buffer, RamcastNode source, RamcastMemoryBlock memoryBlock) {
    this.buffer = buffer;
    this.memoryBlock = memoryBlock;
    this.source = source;
    this.slot = (short) memoryBlock.getTailOffset();
    this.address = memoryBlock.getTail();
  }

  public static int calculateOverhead(int groupCount) {
    return RamcastConfig.SIZE_MSG_ID
            + RamcastConfig.SIZE_MSG_LENGTH
            + RamcastConfig.SIZE_MSG_GROUP_COUNT
            + groupCount * RamcastConfig.SIZE_MSG_GROUP
            + groupCount * RamcastConfig.SIZE_MSG_SLOT
            + RamcastConfig.SIZE_CHECKSUM
            + groupCount * (RamcastConfig.getInstance().getNodePerGroup()) * RamcastConfig.SIZE_ACK
            + RamcastConfig.SIZE_BUFFER_LENGTH;
  }

  public int calculateBasedLength() {
    return RamcastConfig.SIZE_MSG_ID
            + RamcastConfig.SIZE_MSG_LENGTH
            + this.messageLength
            + RamcastConfig.SIZE_MSG_GROUP_COUNT
            + this.getGroupCount() * RamcastConfig.SIZE_MSG_GROUP
            + this.getGroupCount() * RamcastConfig.SIZE_MSG_SLOT
            + RamcastConfig.SIZE_CHECKSUM;
  }

  public ByteBuffer toBuffer() {
    if (this.serializedBuffer == null) this.serializedBuffer = ByteBuffer.allocateDirect(this.calculateBasedLength());
    this.serializedBuffer.clear();
    this.message.clear();
    this.serializedBuffer.putInt(this.id);
    this.serializedBuffer.putInt(this.messageLength);
    this.serializedBuffer.put(this.message);
    this.serializedBuffer.putShort(this.groupsCount);
    for (short i = 0; i < this.groupsCount; i++) {
      this.serializedBuffer.putShort(this.groups[i]);
    }
    for (int i = 0; i < this.groupsCount; i++) {
      this.serializedBuffer.putShort(this.slots[i]);
    }
    int pos = this.serializedBuffer.position();
//    long crc = StringUtils.calculateCrc32((ByteBuffer) this.serializedBuffer.position(0).limit(pos));
    // TODO: Do we need CRC?
    long crc = 1;
    this.serializedBuffer.clear();
    this.serializedBuffer.putLong(pos, crc);
    return this.serializedBuffer;
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

  private int getPosCrc() {
    return getPosSlots() + RamcastConfig.SIZE_MSG_SLOT * getGroupCount();
  }

  private int getPosAcks() {
    return getPosCrc() + RamcastConfig.SIZE_CHECKSUM;
  }

  public short getGroupCount() {
    if (this.groupsCount <= 0) {
      this.groupsCount = this.buffer.getShort(getPosGroupsCount());
    }
    return this.groupsCount;
  }

  public ByteBuffer getMessage() {
    if (this.message == null) {
      this.message = ((ByteBuffer) this.buffer.position(POS_MSG).limit(POS_MSG + this.getMessageLength())).slice();
      this.message.clear();
      this.buffer.clear();
    }
    return this.message;
  }

  // group number
  public short getGroup(int index) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groups[index];
    return this.buffer.getShort(getPosGroups() + 2 * index);
  }

  // slot number in the shared memory of group index
  public short getGroupSlot(int index) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.slots[index];
    try {
      return this.buffer.getShort(getPosSlots() + RamcastConfig.SIZE_MSG_OFFSET * index);
    } catch (Exception e) {
      e.printStackTrace();
      logger.error("{} message {},bufferCap={} limit={},  getPosSlots()={}, getPosSlots() + RamcastConfig.SIZE_MSG_OFFSET * index={}", this, this.buffer.capacity(), this.buffer.limit(), getPosSlots(), (getPosSlots() + RamcastConfig.SIZE_MSG_OFFSET * index));
      throw e;
    }
  }

  // index in the list of groups in the msg of a groupId
  public short getGroupIndex(int groupId) {
    if (groupsMap == null) groupsMap = new HashMap<>();
    if (groupsMap.get((short) groupId) != null) {
      return groupsMap.get((short) groupId);
    }
    for (int i = 0; i < this.getGroupCount(); i++) {
      groupsMap.putIfAbsent(this.getGroup(i), (short) i);
      if (this.getGroup(i) == groupId) return (short) i;
    }
    return -1;
  }

  public int getAckBallot(int groupIndex, int nodeIndex) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groupsAcksBallots[groupIndex][nodeIndex];

    if (this.groupsAcksBallots == null) {
      this.groupsAcksBallots = new int[this.getGroupCount()][config.getNodePerGroup()];
    }
    if (this.groupsAcksBallots[groupIndex][nodeIndex] <= 0) {
      int index = groupIndex * config.getNodePerGroup() + nodeIndex;
      int pos = this.getPosAcks() + RamcastConfig.SIZE_ACK * index;
      try {
        this.groupsAcksBallots[groupIndex][nodeIndex] = this.buffer.getInt(pos);
      } catch (Exception e) {
        // there is a case buffer has not been completed.
        return -1;
      }
    }
    return this.groupsAcksBallots[groupIndex][nodeIndex];
  }

  public int getAckSequence(int groupIndex, int nodeIndex) {
    // this is when the msg has just been created;
    if (this.buffer == null) return this.groupsAckSequences[groupIndex][nodeIndex];

    if (this.groupsAckSequences == null) {
      this.groupsAckSequences = new int[this.getGroupCount()][config.getNodePerGroup()];
    }
    if (this.groupsAckSequences[groupIndex][nodeIndex] <= 0) {
      int index = groupIndex * config.getNodePerGroup() + nodeIndex;
      int pos = this.getPosAcks() + RamcastConfig.SIZE_ACK * index;
      try {
        this.groupsAckSequences[groupIndex][nodeIndex] =
                this.buffer.getInt(pos + RamcastConfig.SIZE_ACK_VALUE);
      } catch (Exception e) {
        // there is a case buffer has not been completed.
        return -1;
      }
    }
    return this.groupsAckSequences[groupIndex][nodeIndex];
  }

  @Override
  public String toString() {
    StringBuilder ret = new StringBuilder();
    if (this.getId() <= 0) return StringUtils.formatMessage("EMPTY");
    ret.append(this.getId())
            .append("║a=")
            .append(this.getAddress())
            .append("║l=")
            .append(this.getMessageLength())
            .append("║")
            .append(this.getMessage().getInt(0))
            .append("..msg║gc=")
            .append(this.getGroupCount())
            .append("║");
    StringBuilder dests = new StringBuilder("g=");
    StringBuilder offset = new StringBuilder("#=");
    StringBuilder acks = new StringBuilder("k=");
    for (int i = 0; i < this.getGroupCount(); i++) dests.append(this.getGroup(i)).append("│");
    for (int i = 0; i < this.getGroupCount(); i++) offset.append(this.getGroupSlot(i)).append("│");

    for (int i = 0; i < this.getGroupCount(); i++) {
      for (int j = 0; j < config.getNodePerGroup(); j++)
        acks.append(this.getAckBallot(i, j))
                .append("/")
                .append(this.getAckSequence(i, j))
                .append("|");
      acks = new StringBuilder(acks.substring(0, acks.length() - 1));
      acks.append("│");
    }
    dests = new StringBuilder(dests.substring(0, dests.length() - 1));
    offset = new StringBuilder(offset.substring(0, offset.length() - 1));
    acks = new StringBuilder(acks.substring(0, acks.length() - 1));
    ret.append(dests)
            .append("║")
            .append(offset)
            .append("║")
            // dont' need to show crc, but it's there
            //        .append(this.getCrc())
            //        .append("║")
            .append(acks);
    return StringUtils.formatMessage(ret.toString());
  }

  public long getCrc() {
    if (this.buffer == null) return 0;
    return buffer.getLong(getPosCrc());
  }

  public ByteBuffer getBuffer() {
    return buffer;
  }

  public void setSlots(short[] slots) {
    this.slots = slots;
  }

  public long getAddress() {
    return address;
  }

  public RamcastMemoryBlock getMemoryBlock() {
    return memoryBlock;
  }

  public int getId() {
    if (this.id <= 0) {
      this.id = ((ByteBuffer) this.buffer.clear()).getInt(POS_ID);
    }
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public void reset() {
    int posAcks = getPosAcks();
    for (int i = 0; i < getGroupCount(); i++) {
      for (int j = 0; j < RamcastConfig.getInstance().getNodePerGroup(); j++) {
        getBuffer().putInt(posAcks, 0);
        getBuffer().putInt(posAcks + 4, 0);
        posAcks += RamcastConfig.SIZE_ACK;
      }
    }
  }

  // CONVENTION: Ack Position of a node depends on its ID
  public int getPosAck(RamcastNode node) {
    int groupIndex = getGroupIndex(node.getGroupId());
    if (logger.isDebugEnabled())
      logger.trace(
              "[{}] getPosAck of node {} groupIndex {} getPosAcks() {}",
              getId(),
              node,
              groupIndex,
              getPosAcks());
    return getPosAcks()
            + groupIndex * RamcastConfig.SIZE_ACK * RamcastConfig.getInstance().getNodePerGroup()
            + node.getNodeId() * RamcastConfig.SIZE_ACK;
  }

  public void writeAck(RamcastNode node, int ballotNumber, int sequenceNumber) {
    int position = getPosAck(node);
    try {
      this.buffer.putInt(position, ballotNumber);
      this.buffer.putInt(position + 4, sequenceNumber);
    } catch (Exception e) {
      System.out.println("Failed to write at position " + position + ". Buffer: " + buffer);
      throw e;
    }
  }

  public short getSlot() {
    return slot;
  }

  public RamcastNode getSource() {
    return this.source;
  }

  public int getFinalTs() {
    return finalTs;
  }

  public void setFinalTs(int finalTs) {
    this.finalTs = finalTs;
  }

  public int getAckBallotNumber(RamcastNode node) {
    int position = getPosAck(node);
    return this.buffer.getInt(position);
  }

  public boolean isAcked(int ballotNumber) {
    for (int i = 0; i < this.getGroupCount(); i++) {
      int count = 0;
      for (int j = 0; j < config.getNodePerGroup(); j++) {
        if (this.getAckBallot(i, j) > 0) count++;
      }
      // todo: change to check majority only
      if (count < RamcastGroup.getQuorum(this.getGroup(i))) {
        return false;
      }
    }
    return true;
  }

  public void addPendingTimestampUpdate() {
    this.pendingTimestampUpdates++;
  }

  public void addCompleteTimestampUpdate() {
    this.completedTimestampUpdates++;
  }

  public int getPendingTimestampUpdates() {
    return pendingTimestampUpdates;
  }

  public int getCompletedTimestampUpdates() {
    return completedTimestampUpdates;
  }
}
