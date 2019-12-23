package ch.usi.dslab.lel.ramcast;

import ch.usi.dslab.lel.ramcast.endpoint.RamcastEndpoint;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ConnectionsTest {
  static Logger logger = LoggerFactory.getLogger(ConnectionsTest.class);
  static RamcastConfig config = RamcastConfig.getInstance();
  static ByteBuffer buffer;

  static int groups = 1;
  static int nodes = 2;
  static Map<RamcastNode, RamcastAgent> agents;
  static List<Thread> threads;

  @BeforeAll
  public static void setUp() throws Exception {
    logger.info("Setting up");
    File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
    config = RamcastConfig.getInstance();
    config.loadConfig(configFile.getPath());

    agents = new ConcurrentHashMap<>();
    threads = new ArrayList<>();

    for (int g = 0; g < groups; g++) {
      for (int p = 0; p < nodes; p++) {
        int finalP = p;
        int finalG = g;
        Thread t =
            new Thread(
                () -> {
                  try {
                    logger.info("Starting " + finalG + "/" + finalP);
                    RamcastAgent agent = new RamcastAgent(finalG, finalP);
                    agents.put(agent.getNode(), agent);
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
    for (RamcastAgent agent : agents.values()) {
      agent.close();
    }
  }

  @Test
  public void verifyConnections() {
    // checking connection count
    for (RamcastAgent agent : agents.values()) {
      assertEquals(groups * nodes - 1, agent.getEndpointMap().keySet().size());
    }
  }

  @Test
  public void verifyDataExchange() {
    // checking exchanged data
    for (RamcastAgent agent : agents.values()) {
      for (Map.Entry<RamcastNode, RamcastEndpoint> connection : agent.getEndpointMap().entrySet()) {
        RamcastEndpoint remoteEndpoint = connection.getValue();
        RamcastNode remoteNode = connection.getKey();
        RamcastAgent remoteAgent = agents.get(remoteNode);
        assertEquals(
            remoteEndpoint.getSharedCircularBlock(),
            remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteSharedCircularBlock());
        assertEquals(
            remoteEndpoint.getSharedTimestampBlock(),
            remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteSharedTimeStampBlock());
        assertEquals(
            remoteEndpoint.getServerHeadBlock(),
            remoteAgent.getEndpointMap().get(agent.getNode()).getClientBlockOfServerHead());
        assertEquals(-1, remoteEndpoint.getServerHeadBlock().getBuffer().get(0));
      }
    }
  }
}
