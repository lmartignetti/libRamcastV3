package ch.usi.dslab.lel.ramcast;

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
import java.util.List;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class WriteSignalTest {
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
  public void testWriteServerHead() throws IOException, InterruptedException {
    assertEquals(-1, agents.get(1).getEndpointMap().get(agents.get(0).getNode()).getRemoteHead());
    int offsetToMove = 9;
    agents
        .get(0)
        .getEndpointGroup()
        .updateRemoteHeadOnClient(
            agents.get(0).getEndpointMap().get(agents.get(1).getNode()), offsetToMove);
    Thread.sleep(10);
    assertEquals(
        offsetToMove, agents.get(1).getEndpointMap().get(agents.get(0).getNode()).getRemoteHead());
  }
}
