package ufrn.kael.distributedPaxos.common.message;

public enum MessageType {

    HEARTBEAT,
    HEARTBEAT_ACK,

    // Business commands
    COMMAND,
    RESPONSE,

    // Paxos
    PREPARE,
    PROMISE,
    ACCEPT,
    ACCEPTED,
    DECISION
}
