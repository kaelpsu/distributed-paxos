package ufrn.kael.distributedPaxos.common.transport;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;

import java.net.InetSocketAddress;
import java.util.Map;

public abstract class AbstractMessageSender implements MessageSender {
    protected final Serializer serializer;
    protected final Map<String, InetSocketAddress> clusterTopology;

    public AbstractMessageSender(Serializer serializer, Map<String, InetSocketAddress> clusterTopology) {
        this.serializer = serializer;
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
            System.err.println("[SENDER] Ignoring unknown node ID: " + nodeId);
            return;
        }

        try {
            byte[] data = serializer.serialize(message);
            
            transmit(data, address);
            
        } catch (Exception e) {
            System.err.println("[SENDER] Failed while sending to " + nodeId + ": " + e.getMessage());
        }
    }

    // each protocol implements its own transmit abstract method
    protected abstract void transmit(byte[] payload, InetSocketAddress address) throws Exception;
}