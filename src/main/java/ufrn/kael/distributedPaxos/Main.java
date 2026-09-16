package ufrn.kael.distributedPaxos;

import ufrn.kael.distributedPaxos.stateless.BusinessNode;
import ufrn.kael.distributedPaxos.common.serialization.JsonSerializer;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.MessageSender;
import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.Protocol;
import ufrn.kael.distributedPaxos.common.transport.grpc.GrpcMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.grpc.GrpcMessageSender;
import ufrn.kael.distributedPaxos.common.transport.tcp.TcpMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.tcp.TcpMessageSender;
import ufrn.kael.distributedPaxos.common.transport.udp.UdpMessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.udp.UdpMessageSender;
import ufrn.kael.distributedPaxos.common.transport.http.HttpMessageSender;
import ufrn.kael.distributedPaxos.gateway.ApiGateway;
import ufrn.kael.distributedPaxos.stateful.DatabaseNode;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    
    // topology matrices (since we only use localhost for these lcoal tests, each "node" is actually a set of three different ports inside the same ip)
    private static final Map<String, InetSocketAddress> tcpTopology = new HashMap<>();
    private static final Map<String, InetSocketAddress> udpTopology = new HashMap<>();
    private static final Map<String, InetSocketAddress> grpcTopology = new HashMap<>();

    public static void main(String[] args) throws Exception {
        JsonSerializer serializer = new JsonSerializer();
        
        // (Offsets: TCP = base, UDP = base + 10, GRPC = base + 30)
        registerNode("gateway-1", 8080);
        registerNode("biz-1", 8081);
        registerNode("biz-2", 8082);
        registerNode("db-1", 9001);
        registerNode("db-2", 9002);
        registerNode("db-3", 9003);

        List<String> databaseNodes = List.of("db-1", "db-2", "db-3");

        DatabaseNode db1 = createDbNode("db-1", 9001, serializer);
        DatabaseNode db2 = createDbNode("db-2", 9002, serializer);
        DatabaseNode db3 = createDbNode("db-3", 9003, serializer);

        BusinessNode biz1 = createBizNode("biz-1", 8081, serializer, databaseNodes);
        BusinessNode biz2 = createBizNode("biz-2", 8082, serializer, databaseNodes);

        NetworkRouter gwRouter = createRouter(serializer);
        List<MessageReceiver> gwReceivers = createReceivers(8080, serializer, 200);
        ApiGateway gateway = new ApiGateway("gateway-1", gwReceivers, gwRouter);

        // 5. Inicialização
        db1.start(); db2.start(); db3.start();
        biz1.start(); biz2.start();
        gateway.start();
        
        System.out.println("Multiprotocol Cluster Multiprotocolo initialized!");
        System.out.println("Gateway listening on: TCP/HTTP(8080) | UDP(8090) | GRPC(8110)");
    }

    private static void registerNode(String id, int basePort) {
        tcpTopology.put(id, new InetSocketAddress("localhost", basePort));
        udpTopology.put(id, new InetSocketAddress("localhost", basePort + 10));
        grpcTopology.put(id, new InetSocketAddress("localhost", basePort + 30));
    }

    private static NetworkRouter createRouter(JsonSerializer serializer) throws Exception {
        Map<Protocol, MessageSender> senders = new HashMap<>();
        senders.put(Protocol.TCP, new TcpMessageSender(tcpTopology));
        senders.put(Protocol.UDP, new UdpMessageSender(udpTopology));
        senders.put(Protocol.HTTP, new HttpMessageSender(tcpTopology));
        senders.put(Protocol.GRPC, new GrpcMessageSender(grpcTopology));
        return new NetworkRouter(senders);
    }

    private static List<MessageReceiver> createReceivers(int basePort, JsonSerializer serializer, int threads) {
        return List.of(
            new TcpMessageReceiver(basePort, serializer, threads),
            new UdpMessageReceiver(basePort + 10, serializer, threads),
            new GrpcMessageReceiver(basePort + 30)
        );
    }

    private static DatabaseNode createDbNode(String id, int port, JsonSerializer serializer) throws Exception {
        return new DatabaseNode(id, createReceivers(port, serializer, 50), createRouter(serializer), 2);
    }

    private static BusinessNode createBizNode(String id, int port, JsonSerializer serializer, List<String> dbNodes) throws Exception {
        return new BusinessNode(
                id, 
                port, // needs to know this for heatbeat
                createReceivers(port, serializer, 50), 
                createRouter(serializer), 
                dbNodes
        );
    }
}