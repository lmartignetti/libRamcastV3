package ch.usi.dslab.lel.ramcast;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class RamcastConfig {
    public static final int MSG_HS_C1 = -1;
    public static final int MSG_HS_S1 = -11;
    public static final int MSG_HS_C2 = -2;
    public static final int MSG_HS_S2 = -12;
    private static RamcastConfig[] instances;

    public static final int SIZE_PACKAGE = 256; // total size of a package, includes payload and overhead
    public static final int SIZE_TIMESTAMP = 12;  //  total size of timestamp, includes timestamp, ballot, value
    public static final int SIZE_REMOTE_HEAD = 8; // total size of remote-head value, include value, msgId

    public static final int SIZE_ACK = 8; // total size of ack message, including ACK, BALLOT
    public static boolean LOG_ENABLED = true;  // flag for logging


    // NODE ROLE
    public static final int ROLE_BOTH = 3;
    public static final int ROLE_CLIENT = 1;
    public static final int ROLE_SERVER = 2;

    private int timeout;

    private int queueLength;
    private int maxinline;
    private int signalInterval;
    private boolean polling;
    private String zkHost;
    private int payloadSize = 32;
    private int groupCount;
    private int nodePerGroup = 1;
    private int reservedSize;
    private int shadowGroupCount;

    private RamcastConfig() {

    }

    public static RamcastConfig getInstance() {
        return getInstance(0);
    }

    public static RamcastConfig getInstance(int index) {
        if (instances == null) {
            instances = new RamcastConfig[10];
        }
        if (instances[index] == null) {
            synchronized (RamcastConfig.class) {
                instances[index] = new RamcastConfig();
            }
        }
        return instances[index];
    }


    static int getJSInt(JSONObject jsobj, String fieldName) {
        return ((Long) jsobj.get(fieldName)).intValue();
    }

    public int getShadowGroupCount() {
        return shadowGroupCount;
    }

    public int getNodePerGroup() {
        return nodePerGroup;
    }


    public int getTotalNodeCount() {
        return RamcastGroup.getTotalNodeCount();
    }

    public int getQueueLength() {
        return queueLength;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }

    public boolean isPolling() {
        return polling;
    }

    public void setPolling(boolean polling) {
        this.polling = polling;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public void setPayloadSize(int payloadSize) {
        this.payloadSize = payloadSize;
    }


    public int getMaxinline() {
        return maxinline;
    }

    public void setMaxinline(int maxinline) {
        this.maxinline = maxinline;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getSignalInterval() {
        return signalInterval;
    }

    public void setSignalInterval(int signalInterval) {
        this.signalInterval = signalInterval;
    }


    public int getReservedSize() {
        return reservedSize;
    }

    public String getZkHost() {
        return zkHost;
    }

    public void loadConfig(String filename) {
        try {

            JSONParser parser = new JSONParser();
            Object nodeObj = parser.parse(new FileReader(filename));
            JSONObject config = (JSONObject) nodeObj;

            if (config.containsKey("queueLength")) {
                this.queueLength = getJSInt(config, "queueLength");
            }
            if (config.containsKey("zkHost")) {
                this.zkHost = String.valueOf(config.get("zkHost"));
            }
            if (config.containsKey("debug")) {
                LOG_ENABLED = (Boolean) (config.get("debug"));
            }
            if (config.containsKey("nodePerGroup")) {
                this.nodePerGroup = getJSInt(config, "nodePerGroup");
            }
            if (config.containsKey("timeout")) {
                this.timeout = getJSInt(config, "timeout");
            }
            if (config.containsKey("signalInterval")) {
                this.signalInterval = getJSInt(config, "signalInterval");
            }
            if (config.containsKey("polling")) {
                this.polling = (Boolean) (config.get("polling"));
            }
            if (config.containsKey("maxinline")) {
                this.maxinline = getJSInt(config, "maxinline");
            }

            // ===========================================
            // Creating Nodes
            JSONArray groupMembersArray = (JSONArray) config.get("group_members");
            Iterator<Object> it_groupMember = groupMembersArray.iterator();
            while (it_groupMember.hasNext()) {
                JSONObject gmnode = (JSONObject) it_groupMember.next();

                int nid = getJSInt(gmnode, "nid");
                int gid = getJSInt(gmnode, "gid");
                int roleId = getJSInt(gmnode, "role");
                String host = (String) gmnode.get("host");
                int port = getJSInt(gmnode, "port");

                RamcastNode node = new RamcastNode(host, port, gid, nid, roleId);
                if (node.hasServerRole()) {
                    RamcastGroup group = RamcastGroup.getOrCreateGroup(gid);
                    group.addNode(node);
                    this.groupCount = RamcastGroup.getGroupCount();
                }
                this.shadowGroupCount = RamcastNode.shadowGroupMap.keySet().size();
            }
            this.reservedSize = groupCount * SIZE_TIMESTAMP + groupCount * nodePerGroup * SIZE_ACK;
//            this.queueLength = nodePerGroup;
        } catch (IOException | ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public int getNodeCount() {
        return this.groupCount * this.nodePerGroup;
    }
}
