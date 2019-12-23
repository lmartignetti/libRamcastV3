package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.CustomHandler;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class WriteMessageTest {
  static Logger logger = LoggerFactory.getLogger(WriteMessageTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static ByteBuffer buffer;
  static List<RamcastAgent> agents;
  Semaphore sendPermits;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up");
    int groups = 1;
    int nodes = 2;
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

    List<RamcastAgent> tmp = new ArrayList<>();
    agents = Collections.synchronizedList(tmp);
    List<Thread> threads = new ArrayList<>();

    for (int g = 0; g < groups; g++) {
      for (int p = 0; p < nodes; p++) {
        int finalP = p;
        int finalG = g;
        Thread t =
            new Thread(
                () -> {
                  try {
                    RamcastAgent agent = new RamcastAgent(finalG, finalP);
                    agents.add(agent);
                    agent.establishConnections();
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
                });
        threads.add(t);
        t.start();
      }
    }
    for (Thread t : threads) {
      t.join();
    }
    logger.info("Setting up DONE");
    logger.info("=============================================");
  }

  @AfterAll
  public static void tearDown() throws IOException, InterruptedException {
    logger.info("Tearing Down");
    for (RamcastAgent agent : agents) {
      agent.close();
    }
  }

  @Test
  @Order(1)
  public void testWriteBigMessage() {
    buffer = ByteBuffer.allocateDirect(249);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');
    Exception exception =
        assertThrows(
            IOException.class,
            () -> agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer));
    String msg = exception.getMessage();
    assertEquals("Buffer size of [249] is too big. Only allow 248", msg);
  }

  @Test
  @Order(2)
  public void testWriteMessage() throws IOException {
    buffer = ByteBuffer.allocateDirect(248);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');

    // make sure the remote buffer has correct number of available slots
    assertEquals(
        RamcastConfig.getInstance().getQueueLength(),
        agents
            .get(0)
            .getEndpointMap()
            .get(agents.get(1).getNode())
            .getRemoteSharedCellBlock()
            .getRemainingSlots());

    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceiveMessage(Object data) {
                assertTrue(
                    buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
              }
            });
    agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer);

    // the number of available slots reduced by 1;
    assertEquals(
        RamcastConfig.getInstance().getQueueLength() - 1,
        agents
            .get(0)
            .getEndpointMap()
            .get(agents.get(1).getNode())
            .getRemoteSharedCellBlock()
            .getRemainingSlots());
  }

//  @Test
//  @Order(4)
//  public void testWriteOnCircularBuffer() throws IOException {
//    buffer = ByteBuffer.allocateDirect(128);
//    buffer.putInt(10);
//    buffer.putInt(11);
//    buffer.putInt(12);
//    buffer.putChar('A');
//
//    // make sure the remote buffer has correct number of available slots
//    assertEquals(
//        RamcastConfig.getInstance().getQueueLength(),
//        agents
//            .get(0)
//            .getEndpointMap()
//            .get(agents.get(1).getNode())
//            .getRemoteSharedCellBlock()
//            .getRemainingSlots());
//  }

  @Test
  @Order(5)
  public void testWriteMultiMessages() throws IOException {
    buffer = ByteBuffer.allocateDirect(248);
    buffer.putInt(10);
    buffer.putInt(11);
    buffer.putInt(12);
    buffer.putChar('A');
    ByteBuffer response = ByteBuffer.allocateDirect(RamcastConfig.SIZE_PAYLOAD);
    response.putInt(100);
    response.putInt(111);
    response.putInt(122);
    response.putChar('A');
    AtomicInteger send = new AtomicInteger(0);
    AtomicInteger receive = new AtomicInteger(0);
    sendPermits = new Semaphore(1);

    agents
        .get(1)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceiveMessage(Object data) {
                assertTrue(
                    buffer.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && buffer.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && buffer.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
                receive.getAndIncrement();
                try {
                  agents.get(1).getEndpointGroup().writeMessage(agents.get(0).getNode(), response);
                } catch (IOException e) {
                  e.printStackTrace();
                }
              }
            });

    agents
        .get(0)
        .getEndpointGroup()
        .setCustomHandler(
            new CustomHandler() {
              @Override
              public void handleReceiveMessage(Object data) {
                assertTrue(
                    response.getInt(0) == ((ByteBuffer) data).getInt(0)
                        && response.getInt(4) == ((ByteBuffer) data).getInt(4)
                        && response.getInt(8) == ((ByteBuffer) data).getInt(8)
                        && response.getChar(12) == ((ByteBuffer) data).getChar(12));
                releasePermit();
              }
            });

    logger.info("Going to run write test for 5 seconds");
    long end = System.currentTimeMillis() + 5 * 1000; // going to run this experiment for 20 secs
    while (send.get() < 99) {
      getPermit();
      agents.get(0).getEndpointGroup().writeMessage(agents.get(1).getNode(), buffer);
      send.getAndIncrement();
    }
    getPermit(); // to wait for the last response
    assertEquals(send.get(), receive.get());
    logger.info("Write {} packages", send.get() + receive.get());
  }

  void getPermit() {
    try {
      sendPermits.acquire();
    } catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void releasePermit() {
    sendPermits.release();
  }
}
