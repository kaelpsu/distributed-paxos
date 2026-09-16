package ufrn.kael.distributedPaxos.common.message;

public record Heartbeat(
    String messageId,
    String senderId,
    String targetId,
    String originId
) implements Message {}
