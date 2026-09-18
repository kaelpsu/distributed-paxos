package ufrn.kael.distributedPaxos.common.transport.grpc;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.GrpcMessage;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc.TransportServiceStub;

public class GrpcMessageSender extends AbstractMessageSender {

    private final Map<String, ManagedChannel> channelCache = new ConcurrentHashMap<>();
    private final Map<String, TransportServiceStub> asyncStubCache = new ConcurrentHashMap<>();

    public GrpcMessageSender(Map<String, InetSocketAddress> clusterTopology) {
        super(clusterTopology);
    }

    @Override
    protected void transmit(Message message, InetSocketAddress address) throws Exception {

        String targetKey = address.getHostString() + ":" + address.getPort();

        TransportServiceStub stub = asyncStubCache.computeIfAbsent(targetKey, k -> {

            ManagedChannel channel = ManagedChannelBuilder.forAddress(address.getHostString(), address.getPort()).usePlaintext().build();
            
            channelCache.put(targetKey, channel);
            return TransportServiceGrpc.newStub(channel);
        });

        GrpcMessage request = GrpcMapper.toGrpc(message);

        // avoids that request get cancelled after origin server thread is finished
        io.grpc.Context.ROOT.run(() -> {
            stub.withDeadlineAfter(1500, TimeUnit.MILLISECONDS).transmitMessage(request, new StreamObserver<GrpcMessage>() {
                @Override
                public void onNext(GrpcMessage value) {}

                @Override
                public void onError(Throwable t) {
                    System.err.println("[GRPC SENDER] Failed on route " + targetKey + ": " + t.getMessage());
                }

                @Override
                public void onCompleted() {}
            });
        });
    }
    
    public void shutdown() {
        channelCache.values().forEach(channel -> channel.shutdownNow());
    }
}