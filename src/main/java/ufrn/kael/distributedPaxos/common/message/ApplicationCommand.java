package ufrn.kael.distributedPaxos.common.message;

public record ApplicationCommand(
        String messageId,
        String senderId,
        String targetId,
        String originId,
        Transaction transaction
) implements Message {
}