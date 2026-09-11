package ufrn.kael.distributedPaxos.common;

import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;

import java.util.List;

public abstract class Node {
    protected final String nodeId;
    protected final List<MessageReceiver> receivers; // each protocol has its own receiver
    protected final NetworkRouter router; // each protocol has its own sender (this class manages which one to use)

    protected Node(String nodeId, List<MessageReceiver> receivers, NetworkRouter router) {
        this.nodeId = nodeId;
        this.receivers = receivers;
        this.router = router;
    }

    public String nodeId() {
        return nodeId;
    }

    public abstract void start();

    public abstract void stop();
}
