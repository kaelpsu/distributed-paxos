package ufrn.kael.distributedPaxos.common.loadbalancing;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinBalancer {
    
    private final AtomicInteger counter = new AtomicInteger(0);

    public String getNextNode(List<String> activeNodes) {
        if (activeNodes == null || activeNodes.isEmpty()) {
            throw new IllegalStateException("Service Unavailable: There are no active Business Nodes.");
        }
        
        int index = counter.getAndIncrement() % activeNodes.size();
        return activeNodes.get(index);
    }
}