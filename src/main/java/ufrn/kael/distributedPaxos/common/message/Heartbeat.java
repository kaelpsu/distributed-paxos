package ufrn.kael.distributedPaxos.common.message;

public record Heartbeat(
    String messageId,
    String senderId,
    String targetId,
    String originId,
    String ipAddress,
    int tcpPort,    // we only need these for 
    int udpPort,    // local testing, since the ports 
    int httpPort    // will be pre-defined on real networks
) implements Message {}
