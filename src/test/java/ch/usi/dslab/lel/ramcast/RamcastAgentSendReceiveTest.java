package ch.usi.dslab.lel.ramcast;


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RamcastAgentSendReceiveTest {
    static RamcastConfig config = RamcastConfig.getInstance();
    static ByteBuffer buffer;
    static List<RamcastAgent> agents;
    Semaphore sendPermits;

    @BeforeAll
    public static void setUp() throws Exception {
        System.out.println("Setting up");
        int groups = 1;
        int nodes = 2;
        File configFile = new File("src/test/resources/systemConfig" + groups + "g" + nodes + "p.json");
        config = RamcastConfig.getInstance();
        config.loadConfig(configFile.getPath());

        buffer = ByteBuffer.allocateDirect(RamcastConfig.SIZE_PACKAGE);
        buffer.putInt(10);
        buffer.putInt(11);
        buffer.putInt(12);
        buffer.putChar('A');

        List<RamcastAgent> tmp = new ArrayList<>();
        agents = Collections.synchronizedList(tmp);
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

    @AfterAll
    public static void tearDown() throws IOException, InterruptedException {
        System.out.println("Tearing Down");
        for (RamcastAgent agent : agents) {
            agent.close();
        }
//        Thread.sleep(1000);
    }

    @Test
    public void testSend() throws IOException {
        agents.get(0).getEndpointGroup().setCustomCallback(data -> {
            assertTrue(buffer.getInt(0) == ((ByteBuffer) data).getInt(0) &&
                    buffer.getInt(4) == ((ByteBuffer) data).getInt(4) &&
                    buffer.getInt(8) == ((ByteBuffer) data).getInt(8) &&
                    buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
        });
        agents.get(0).send(agents.get(1).getNode(), buffer);
        agents.get(0).send(agents.get(1).getNode(), buffer);
        agents.get(0).send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
    }

    @Test
    public void testRecive() throws IOException {
        agents.get(1).getEndpointGroup().setCustomCallback(data -> {
            assertTrue(buffer.getInt(0) == ((ByteBuffer) data).getInt(0) &&
                    buffer.getInt(4) == ((ByteBuffer) data).getInt(4) &&
                    buffer.getInt(8) == ((ByteBuffer) data).getInt(8) &&
                    buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
        });
        agents.get(0).send(agents.get(1).getNode(), buffer);
        agents.get(0).send(agents.get(1).getNode(), buffer);
        agents.get(0).send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
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


//    @Test
//    public void testMassSend() throws IOException, InterruptedException {
//        sendPermits = new Semaphore(1);
//        AtomicInteger send = new AtomicInteger(0);
//        AtomicInteger receive = new AtomicInteger(0);
//        agents.get(0).getEndpointGroup().setCustomCallback(data -> {
//            assertTrue(buffer.getInt(0) == ((ByteBuffer) data).getInt(0) &&
//                    buffer.getInt(4) == ((ByteBuffer) data).getInt(4) &&
//                    buffer.getInt(8) == ((ByteBuffer) data).getInt(8) &&
//                    buffer.getChar(12) == ((ByteBuffer) data).getChar(12));
//            receive.getAndIncrement();
//            releasePermit();
//        });
//        while (send.get() < 100) {
//            getPermit();
//            agents.get(0).send(agents.get(1).getNode().getGroupId(), agents.get(1).getNode().getNodeId(), buffer);
//            send.getAndIncrement();
//        }
//        Thread.sleep(500); // wait for the remaining callback
//        assertEquals(send.get(), receive.get());
//
//    }


}
