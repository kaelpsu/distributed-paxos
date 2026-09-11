package ufrn.kael.distributedPaxos.common.transport;

import ufrn.kael.distributedPaxos.common.message.Message;
import java.util.Map;

// this class will choose the adequate sender to a given protocol
public class NetworkRouter {
    private final Map<Protocol, MessageSender> senders;

    public NetworkRouter(Map<Protocol, MessageSender> senders) {
        this.senders = senders;
    }

    public void send(Message message, Protocol protocol) {
        MessageSender sender = senders.get(protocol);
        if (sender != null) {
            sender.send(message);
        } else {
            System.err.println("[ROUTER] No sender implemented for protocol: " + protocol);
        }
    }
}