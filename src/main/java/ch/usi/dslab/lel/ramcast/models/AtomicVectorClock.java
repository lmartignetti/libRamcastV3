package ch.usi.dslab.lel.ramcast.models;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicVectorClock {
  private static final int SIZE_GROUP_ID = 1;
  private static final int SIZE_CLOCK_VALUE = 3;
  private int groupId;
  private AtomicInteger value;

  public AtomicVectorClock(int groupId, int value) {
    this.groupId = groupId;
    this.value = new AtomicInteger(value);
  }

  public AtomicVectorClock(int groupId) {
    this(groupId, 0);
  }

  public static AtomicVectorClock parse(int clock) {
    return new AtomicVectorClock(clock << 8 * SIZE_CLOCK_VALUE >> 8 * SIZE_CLOCK_VALUE, clock >> 8 * SIZE_GROUP_ID);
  }

  public int get() {
    return (value.get() << 8 * SIZE_GROUP_ID) | groupId;
  }

  public int get(int v) {
    return (v << 8 * SIZE_GROUP_ID) | groupId;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getValue() {
    return value.get();
  }

  public boolean compareAndSet(int expect, int newValue) {
    return this.value.compareAndSet(expect >> 8 * SIZE_GROUP_ID, newValue >> 8 * SIZE_GROUP_ID);
  }

  public int incrementAndGet() {
    int v = this.value.incrementAndGet();
    return this.get(v);
  }
}

/*

int k = 5;
int x = 3;


                  +---+---+---+
                k:| 1 | 0 | 1 |
                  +---+---+---+
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+

      +---+---+---+---+---+---+
k<<=3:| 1 | 0 | 1 | 0 | 0 | 0 |
      +---+---+---+---+---+---+
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+

      +---+---+---+---+---+---+
 k|=3:| 1 | 0 | 1 | 0 | 1 | 1 |
      +---+---+---+---+---+---+
                    ^   ^   ^
                  +---+---+---+
                x:| 0 | 1 | 1 |
                  +---+---+---+
 */