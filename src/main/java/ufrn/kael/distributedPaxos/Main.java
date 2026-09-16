package ufrn.kael.distributedPaxos;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.MessageSender;
import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.Protocol;
import ufrn.kael.distributedPaxos.common.transport.grpc.GrpcMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.grpc.GrpcMessageSender;
import ufrn.kael.distributedPaxos.common.transport.http.HttpMessageSender;
import ufrn.kael.distributedPaxos.common.transport.tcp.TcpMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.tcp.TcpMessageSender;
import ufrn.kael.distributedPaxos.common.transport.udp.UdpMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.udp.UdpMessageSender;
import ufrn.kael.distributedPaxos.gateway.ApiGateway;
import ufrn.kael.distributedPaxos.stateful.DatabaseNode;
import ufrn.kael.distributedPaxos.stateless.BusinessNode;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {

    // hard-coded ports
    private static final int PORT_TCP_HTTP = 8080;
    private static final int PORT_UDP = 9090;
    private static final int PORT_GRPC = 50051;

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        
        String nodeType = params.get("--type");
        String nodeId = params.get("--id");
        String configFile = params.getOrDefault("--config", "topology.json");

        if (nodeType == null || nodeId == null) {
            System.err.println("Usage: java -jar app.jar --type <gateway|biz|db> --id <node-id> [--config topology.json]");
            System.exit(1);
        }

        Map<String, String> ipTopology = loadTopologyIps(configFile);
        if (!ipTopology.containsKey(nodeId)) {
            throw new IllegalArgumentException("Node " + nodeId + " not found on the topology!");
        }

        Map<String, InetSocketAddress> tcpTopology = buildAddressMap(ipTopology, PORT_TCP_HTTP);
        Map<String, InetSocketAddress> udpTopology = buildAddressMap(ipTopology, PORT_UDP);
        Map<String, InetSocketAddress> grpcTopology = buildAddressMap(ipTopology, PORT_GRPC);

        Map<Protocol, MessageSender> senders = new HashMap<>();
        senders.put(Protocol.TCP, new TcpMessageSender(tcpTopology));
        senders.put(Protocol.HTTP, new HttpMessageSender(tcpTopology));
        senders.put(Protocol.UDP, new UdpMessageSender(udpTopology));
        senders.put(Protocol.GRPC, new GrpcMessageSender(grpcTopology));
        
        NetworkRouter router = new NetworkRouter(senders);

        List<MessageReceiver> sharedReceivers = List.of(
                new TcpMessageReceiver(8080, 50),
                new UdpMessageReceiver(9090, 50),
                new GrpcMessageReceiver(50051)
        );

        System.out.println("Iinitializing node [" + nodeId + "] of type [" + nodeType + "]...");
        System.out.println("Protocols: TCP/HTTP=" + PORT_TCP_HTTP + " | UDP=" + PORT_UDP + " | GRPC=" + PORT_GRPC);

        switch (nodeType.toLowerCase()) {
            case "gateway":
                new ApiGateway(nodeId, sharedReceivers, router).start();
                break;
            case "biz":
                new BusinessNode(nodeId, sharedReceivers, router).start();
                break;
            case "db":
                new DatabaseNode(nodeId, sharedReceivers, router, 2).start();
                break;
            default:
                System.err.println("Invalid type. Use: gateway, biz or db.");
                System.exit(1);
        }
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> params = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            if (i + 1 < args.length && args[i].startsWith("--")) {
                params.put(args[i], args[i + 1]);
            }
        }
        return params;
    }

    private static Map<String, String> loadTopologyIps(String path) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new File(path), new TypeReference<Map<String, String>>() {});
    }

    private static Map<String, InetSocketAddress> buildAddressMap(Map<String, String> ips, int port) {
        Map<String, InetSocketAddress> map = new HashMap<>();
        ips.forEach((id, ip) -> map.put(id, new InetSocketAddress(ip, port)));
        return map;
    }
}