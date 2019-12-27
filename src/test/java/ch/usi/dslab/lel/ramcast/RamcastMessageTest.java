package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.models.RamcastGroup;
import ch.usi.dslab.lel.ramcast.models.RamcastMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Semaphore;
import static org.junit.jupiter.api.Assertions.*;

public class RamcastMessageTest {
  static Logger logger = LoggerFactory.getLogger(RamcastMessageTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static List<RamcastAgent> agents;
  Semaphore lock;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up for RamcastMessageTest");
    int groups = 2;
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
    agents.sort(Comparator.comparingInt(agents -> agents.getNode().getOrderId()));
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
  public void testCreatingMessage() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);
    buffer.putInt(100);
    buffer.putChar('A');
    buffer.putLong(10);
    List<RamcastGroup> dests = new ArrayList<>();
    dests.add(RamcastGroup.getGroup(0));
    RamcastMessage message = agents.get(0).createMessage(buffer, dests);
    logger.debug("Created message: \n{}", message);
    assertEquals(1, message.getGroupCount());
    assertEquals(0, message.getGroup(0));

    dests = new ArrayList<>();
    dests.add(RamcastGroup.getGroup(0));
    dests.add(RamcastGroup.getGroup(1));
    message = agents.get(0).createMessage(buffer, dests);
    logger.debug("Created message: \n{}", message);
    assertEquals(2, message.getGroupCount());
    assertEquals(0, message.getGroup(0));
    assertEquals(1, message.getGroup(1));

  }

  @Test
  public void testContinouslyCreateAndMessage() throws IOException, InterruptedException {
    RamcastAgent agent0 = agents.get(0);
    ByteBuffer buffer = ByteBuffer.allocateDirect(128);
    buffer.putInt(100);
    buffer.putChar('A');
    buffer.putLong(10);
    List<RamcastGroup> dests = new ArrayList<>();
    dests.add(RamcastGroup.getGroup(0));
    RamcastMessage message = agent0.createMessage(buffer, dests);
    logger.debug("Created message: \n{}", message);
    assertEquals(1, message.getGroupCount());
    assertEquals(0, message.getGroup(0));

    agent0.multicast(message, dests);
    Thread.sleep(1000);

  }
}
