package ufrn.kael.distributedPaxos.common.message;

public record ApplicationResponse(
        String messageId,
        String senderId,
        String targetId,
        String originId,
        boolean success,
        String message
) implements Message {
}