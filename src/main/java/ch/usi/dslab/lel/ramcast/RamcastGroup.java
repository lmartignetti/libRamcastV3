package ch.usi.dslab.lel.ramcast;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class RamcastGroup {
    static ArrayList<RamcastGroup> groupList;
    static HashMap<Integer, RamcastGroup> groupMap;


    static {
        groupList = new ArrayList<>();
        groupMap = new HashMap<>();
    }

    int groupId;
    private HashMap<Integer, RamcastNode> nodeMap;
    private boolean isShadow;
    private RamcastNode leader;

    public RamcastGroup(int groupId) {
        this.groupId = groupId;
        groupList.add(this);
        this.nodeMap = new HashMap<>();

    }

    public static int getGroupCount() {
        return groupList.size();
    }

    public static RamcastGroup getOrCreateGroup(int id) {
        RamcastGroup g = groupMap.get(id);
        if (g == null) {
            g = new RamcastGroup(id);
            groupMap.put(id, g);
        }
        return g;
    }

    public static RamcastGroup getGroup(int id) throws RuntimeException {
        RamcastGroup g = groupMap.get(id);
        assert g != null;
        return g;
    }

    public static List<RamcastNode> getAllNodes() {
        return groupList.stream().map(RamcastGroup::getMembers).flatMap(List::stream).collect(Collectors.toList());
    }

    public RamcastNode getNode(int nodeId) {
        return nodeMap.get(nodeId);
    }

    public void addNode(RamcastNode node) {
        nodeMap.put(node.getNodeId(), node);
    }

    public void removeNode(RamcastNode node) {
        nodeMap.remove(node.getNodeId());
    }

    public static int getTotalNodeCount() {
        return groupList.stream().mapToInt(RamcastGroup::getNodeCount).sum();
    }

    public int getNodeCount() {
        return nodeMap.keySet().size();
    }

    //    public List<Integer> getMembers() {
//        return nodeMap.entrySet().stream().map(entry -> entry.getValue().getNodeId()).collect(Collectors.toList());
//    }
    public List<RamcastNode> getMembers() {
        return new ArrayList<>(nodeMap.values());
    }

    public int getId() {
        return groupId;
    }

    public RamcastNode getLeader() {
        return this.leader;
    }

    public void setLeader(RamcastNode leader) {
        this.leader = leader;
    }

    @Override
    public String toString() {
        return "[group " + this.groupId + "]";
    }
}
