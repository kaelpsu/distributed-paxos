package ufrn.kael.distributedPaxos.common.transport;

import ufrn.kael.distributedPaxos.common.message.Message;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class AbstractMessageSender implements MessageSender {
    protected final Map<String, InetSocketAddress> clusterTopology;

    public AbstractMessageSender(Map<String, InetSocketAddress> clusterTopology) {
        this.clusterTopology = clusterTopology;
    }

    @Override
    public void send(Message message) {
        // checks if its a broadcast
        if ("*".equals(message.targetId())) {
            for (String nodeId : clusterTopology.keySet()) {
                sendTo(message, nodeId);
            }
            return;
        }
        sendTo(message, message.targetId());
    }

    private void sendTo(Message message, String nodeId) {
        InetSocketAddress address = clusterTopology.get(nodeId);

        if (address == null) {
            // ignores unknown id
            return;
        }

        try {
            
            transmit(message, address);
            
        } catch (Exception e) {
            System.err.println("[SENDER] Failed while sending to " + nodeId + ": " + e.getMessage());
        }
    }

    // each protocol implements its own transmit abstract method
    protected abstract void transmit(Message message, InetSocketAddress address) throws Exception;
}