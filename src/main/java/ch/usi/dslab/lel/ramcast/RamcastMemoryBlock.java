package ch.usi.dslab.lel.ramcast;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Objects;
import java.util.SortedSet;
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
    private SortedSet<FreeSegment> pendingFreeSegnment = new TreeSet<>(new FreeSegmentComparator());
    private RamcastMemoryBlock _origin;

    public RamcastMemoryBlock() {
    }

    @Override
    public String toString() {
        return "RamcastMemoryBlock{" +
                "address=" + address +
                ", lkey=" + lkey +
                ", capacity=" + capacity +
                '}';
    }

    public RamcastMemoryBlock(long address, int lkey, int capacity, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.capacity = capacity;
        this.headOffset = 0;
        this.tailOffset = 0;
        this.buffer = buffer;
    }

    public void update(long address, int lkey, int length, ByteBuffer buffer) {
        this.address = address;
        this.lkey = lkey;
        this.capacity = length;
        this.headOffset = 0;
        this.tailOffset = 0;
        this.buffer = buffer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RamcastMemoryBlock that = (RamcastMemoryBlock) o;

        if (address != that.address) return false;
        if (lkey != that.lkey) return false;
        return capacity == that.capacity;
    }

    @Override
    public int hashCode() {
        int result = (int) (address ^ (address >>> 32));
        result = 31 * result + lkey;
        result = 31 * result + capacity;
        return result;
    }

    public long getHead() {
        return address + headOffset;
    }

    public int getHeadOffset() {
        return headOffset;
    }

    public void setHeadOffset(int newOffset) {
        // if new offset less then current offset -> it was reset on server.
        // if the remaining less than capacity -> reset too
//        if (newOffset < this.headOffset || remainingCapacity() < minSegmentSize) {
        if (newOffset < this.headOffset) {
            this.reset();
        }
        this.headOffset = newOffset;
    }

    public long getTail() {
        return address + tailOffset;
    }

    public long getTail(int minSize) {
        return address + tailOffset;
    }

    public int getTailOffset() {
        return tailOffset;
    }

    public void setTailOffset(int offset) {
        this.tailOffset = offset;
    }

    public int remainingCapacity() {
        return capacity - tailOffset;
    }

    public boolean moveTail(int length) {
        if (length <= capacity - tailOffset) {
            tailOffset += length;

//            if (capacity - tailOffset < minSegmentSize) {
//
//            }
            return true;
        } else {
            logger.error("ERROR MOVING TAIL: head {} length {} tail {} capacity {}", headOffset, length, tailOffset, capacity);
            System.exit(-1);
            return false;
        }
    }

    // TODO: remove system.exit
    public boolean moveHead(int length) {
        if (length == 0) {// reset case
            this.reset();
            return true;
        } else if (length <= capacity - headOffset && headOffset + length <= tailOffset) {
            this.headOffset += length;
            if (capacity - headOffset < RamcastConfig.SIZE_PACKAGE) {
                if (headOffset > tailOffset || headOffset < tailOffset) {
                    logger.error("ERROR MOVING HEAD: at the end HEAD not equal TAIL: head {} length {} tail {} capacity {}", headOffset, length, tailOffset, capacity);
                    System.exit(-1);
                }
                this.reset();
            }
            return true;
        } else {
            logger.error("ERROR MOVING HEAD: head {} length {} tail {} capacity {}", headOffset, length, tailOffset, capacity);
            System.exit(-1);
            return false;
        }
    }

    public void reset() {
        this.headOffset = 0;
        this.tailOffset = 0;
    }

    public RamcastEndpoint getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(RamcastEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public void freeSegment(long start, long end) {
        if (this.headOffset + this.address == start) {
            this.moveHead((int) (end - start));
        } else {
            this.pendingFreeSegnment.add(new FreeSegment(start, end));
        }
    }

    public ByteBuffer getBuffer() {
        return buffer;
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
        block._origin = this;
        return block;
    }

    public RamcastMemoryBlock getOrigin() {
        return _origin;
    }

    class FreeSegment {
        long start;
        long end;

        public FreeSegment(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.start, this.end);
        }

        @Override
        public boolean equals(Object obj) {
            return this.start == ((FreeSegment) obj).start && this.end == ((FreeSegment) obj).end;
        }
    }

    class FreeSegmentComparator implements Comparator<FreeSegment> {
        public int compare(FreeSegment s1, FreeSegment s2) {
            return (int) (s1.start - s2.start);
        }
    }
}
