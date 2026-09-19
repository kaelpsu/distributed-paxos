package ufrn.kael.distributedPaxos.common.transport.grpc;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.GrpcMessage;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc.TransportServiceBlockingStub;

public class GrpcMessageSender extends AbstractMessageSender {

    private final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();
    private final Map<String, TransportServiceBlockingStub> blockingStubCache = new ConcurrentHashMap<>();

    public GrpcMessageSender(Map<String, InetSocketAddress> clusterTopology) {
        super(clusterTopology);
    }

    @Override
    protected void transmit(Message message, InetSocketAddress address) throws Exception {

        String targetKey = address.getHostString() + ":" + address.getPort();

        TransportServiceBlockingStub stub = blockingStubCache.computeIfAbsent(targetKey, k -> {

            ManagedChannel channel = ManagedChannelBuilder.forAddress(address.getHostString(), address.getPort()).usePlaintext().build();
            
            channelCache.put(targetKey, channel);
            return TransportServiceGrpc.newBlockingStub(channel);
        });

        GrpcMessage request = GrpcMapper.toGrpc(message);

        try {
            stub.withDeadlineAfter(1500, TimeUnit.MILLISECONDS).transmitMessage(request);
            
        } catch (StatusRuntimeException e) {
            throw new RuntimeException("NETWORK_ERROR: Unreachable target node (" + address.getHostName() + ":" + address.getPort() + ")", e);
        }
    }
    
    public void shutdown() {
        channelCache.values().forEach(channel -> channel.shutdownNow());
        channelCache.clear();
        blockingStubCache.clear();
    }
}