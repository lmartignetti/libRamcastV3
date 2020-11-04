package ch.usi.dslab.lel.ramcast.models;

import java.util.concurrent.atomic.AtomicInteger;

public class AtomicVectorClock {
  private int groupId;
  private AtomicInteger value;
  private AtomicInteger latest;

  public AtomicVectorClock(int groupId, int value) {
    this.groupId = groupId;
    this.value = new AtomicInteger(value);
    this.latest = new AtomicInteger(value);
  }

  public AtomicVectorClock(int groupId) {
    this(groupId, 0);
  }

  public static AtomicVectorClock parse(int clock) {
    return new AtomicVectorClock(clock >> 8 * 3, clock << 8 >> 8);
  }

  public int get() {
    return (groupId << 8 * 3) | value.get();
  }

  public int get(int v) {
    return (groupId << 8 * 3) | v;
  }

  public int getGroupId() {
    return groupId;
  }

  public int getValue() {
    return value.get();
  }

  public void setValue(int value) {
    this.value.set(value);
  }

  public int incrementAndGet() {
//    int latest = this.latest.get();
    int value = this.value.incrementAndGet();
//    while (value <= latest) value = this.value.incrementAndGet();
//    this.latest.set(value);
    return this.get(value);
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