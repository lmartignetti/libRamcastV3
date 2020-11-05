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
    return new AtomicVectorClock(clock << 24 >> 24, clock >> 8 * 1);
  }

  public synchronized int get() {
    return (value.get() << 8 * 1) | groupId;
  }

  public synchronized int get(int v) {
    return (v << 8 * 1) | groupId;
  }

  public synchronized int getGroupId() {
    return groupId;
  }

  public synchronized int getValue() {
    return value.get();
  }

  public synchronized void setValue(int value) {
    this.value.set(value >> 8 * 1);
  }

  public synchronized int incrementAndGet() {
//    int latest = this.latest.get();
    int v = this.value.incrementAndGet();
//    System.out.println("Group=" + groupId + " latest=" + latest + " value=" + value);
//    while (v <= latest) {
//      latest = this.latest.get();
//      v = this.value.incrementAndGet();
//    }
//    this.latest.set(v);
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