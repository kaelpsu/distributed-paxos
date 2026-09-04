package ufrn.kael.distributedPaxos.common.message;

import java.util.Objects;
import java.util.UUID;

public record Message<T>(
        UUID id,
        NodeId sender,
        NodeId receiver,
        MessageType type,
        T payload
) {

    public Message {
        Objects.requireNonNull(id, "id cannot be null.");
        Objects.requireNonNull(sender, "sender cannot be null.");
        Objects.requireNonNull(receiver, "receiver cannot be null.");
        Objects.requireNonNull(type, "type cannot be null.");
        Objects.requireNonNull(payload, "payload cannot be null.");
    }

    /**
     * Factory method to create a new Message instance with a unique ID.
     */
    public static <T> Message<T> create(
            NodeId sender,
            NodeId receiver,
            MessageType type,
            T payload
    ) {
        return new Message<>(
                UUID.randomUUID(),
                sender,
                receiver,
                type,
                payload
        );
    }
}
