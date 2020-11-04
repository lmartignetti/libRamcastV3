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

  public synchronized int get() {
    return (groupId << 8 * 3) | value.get();
  }

  public synchronized int get(int v) {
    return (groupId << 8 * 3) | v;
  }

  public synchronized int getGroupId() {
    return groupId;
  }

  public synchronized int getValue() {
    return value.get();
  }

  public synchronized void setValue(int value) {
    this.value.set(value);
  }

  public synchronized int incrementAndGet() {
    int latest = this.latest.get();
    int v = this.value.incrementAndGet();
    while (v <= latest) {
      latest = this.latest.get();
      v = this.value.incrementAndGet();
    }
    this.latest.set(v);
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