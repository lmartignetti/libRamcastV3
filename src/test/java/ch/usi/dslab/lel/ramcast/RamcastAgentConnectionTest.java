package ch.usi.dslab.lel.ramcast;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RamcastAgentConnectionTest {
    static RamcastConfig config = RamcastConfig.getInstance();
    static ByteBuffer buffer;
    static boolean setUpIsDone = false;

    static int groups = 1;
    static int nodes = 2;
    static Map<RamcastNode, RamcastAgent> agents;
    static List<Thread> threads;

    @BeforeAll
    public static void setUp() throws Exception {
        if (setUpIsDone) {
            return;
        }
        System.out.println("Setting up");
        File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
        config = RamcastConfig.getInstance();
        config.loadConfig(configFile.getPath());

        agents = new ConcurrentHashMap<>();
        threads = new ArrayList<>();

        for (int g = 0; g < groups; g++) {
            for (int p = 0; p < nodes; p++) {
                int finalP = p;
                int finalG = g;
                Thread t = new Thread(() -> {
                    try {
                        System.out.println("Starting " + finalG + "/" + finalP);
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
        setUpIsDone = true;
    }

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        System.out.println("Tearing Down");
        for (RamcastAgent agent : agents.values()) {
            agent.close();
        }
//        Thread.sleep(1000);
    }

    @Test
    public void testConnections() {
        // checking connection count
        for (RamcastAgent agent : agents.values()) {
            assertEquals(groups * nodes - 1, agent.getEndpointMap().keySet().size());
        }

        //checking exchanged data
        for (RamcastAgent agent : agents.values()) {
//            System.out.println("Agents:" + agents);
//            System.out.println("Checking agent: " + agent);
            for (Map.Entry<RamcastNode, RamcastEndpoint> connection : agent.getEndpointMap().entrySet()) {
                RamcastEndpoint remoteEndpoint = connection.getValue();
                RamcastNode remoteNode = connection.getKey();
                RamcastAgent remoteAgent = agents.get(remoteNode);
//                System.out.println("Remote node:" + remoteNode);
//                System.out.println("Remote agent:" + remoteAgent);
//                System.out.println("Current agent memory:" + remoteEndpoint.getSharedCircularBlock());
//                System.out.println("Remote agent:" + remoteAgent.getEndpointMap().get(agent.getNode()));
//                System.out.println("Remote agent:" + remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteSharedCircularBlock());
                assertEquals(remoteEndpoint.getSharedCircularBlock(), remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteSharedCircularBlock());
                assertEquals(remoteEndpoint.getSharedTimestampBlock(), remoteAgent.getEndpointMap().get(agent.getNode()).getRemoteSharedTimeStampBlock());
                assertEquals(remoteEndpoint.getServerHeadBlock(), remoteAgent.getEndpointMap().get(agent.getNode()).getClientBlockOfServerHead());

            }
        }
    }

}