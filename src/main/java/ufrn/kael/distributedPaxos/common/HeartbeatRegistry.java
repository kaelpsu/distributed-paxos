package ufrn.kael.distributedPaxos.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HeartbeatRegistry {
    
    private final Map<String, Long> lastSeen = new ConcurrentHashMap<>();

    public void registerOrUpdateNode(String nodeId) {
        if (lastSeen.get(nodeId) == null) {
            System.out.println("[REGISTRY] Registered node: " + nodeId);
        }
        lastSeen.put(nodeId, System.currentTimeMillis());
    }

    public void removeDeadNodes(long timeoutMs) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : lastSeen.entrySet()) {
            if (now - entry.getValue() > timeoutMs) {
                String deadNode = entry.getKey();
                lastSeen.remove(deadNode);
                System.err.println("[REGISTRY] Declared node as DEAD for inactivity: " + deadNode);
            }
        }
    }

    // useful methods for load balancer
    public List<String> getActiveBusinessNodes() {
        return lastSeen.keySet().stream()
                .filter(id -> id.startsWith("biz-"))
                .toList();
    }

    public List<String> getActiveDatabaseNodes() {
        return lastSeen.keySet().stream()
                .filter(id -> id.startsWith("db-"))
                .toList();
    }
}