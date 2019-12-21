package ch.usi.dslab.lel.ramcast;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RamcastAgentSendReceiveTest {
    RamcastConfig config = RamcastConfig.getInstance();
    ByteBuffer buffer;
    private static boolean setUpIsDone = false;

    private List<RamcastAgent> agents;

    @Before
    public void setUp() throws Exception {
        if (setUpIsDone) {
            return;
        }
        System.out.println("Setting up");
        int groups = 1;
        int nodes = 2;
        File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
        config = RamcastConfig.getInstance();
        config.loadConfig(configFile.getPath());

        buffer = ByteBuffer.allocateDirect(60);
        buffer.putInt(10);
        buffer.putInt(11);
        buffer.putInt(12);
        buffer.putChar('A');
        buffer.putChar('B');
        buffer.putChar('C');

        agents = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();

        for (int g = 0; g < groups; g++) {
            for (int p = 0; p < nodes; p++) {
                int finalP = p;
                int finalG = g;
                Thread t = new Thread(() -> {
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
        System.out.println("Setting up DONE");
        System.out.println("=============================================");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSendOnce() throws IOException {
        agents.get(0).send(agents.get(1).getNode(), buffer);
        agents.get(0).send(0, 1, buffer);
        agents.get(1).send(agents.get(0).getNode(), buffer);
    }

//    @Test
//    public void testSendMultiple() {
//        ExceptionAssertion.assertDoesNotThrow(() -> {
//            for (int i = 0; i < 100; i++) {
//                agents.get(0).send(agents.get(1).getNode(), buffer);
//            }
//        });
//    }
}