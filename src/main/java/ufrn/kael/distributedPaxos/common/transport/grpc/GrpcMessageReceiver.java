package ufrn.kael.distributedPaxos.common.transport.grpc;

import java.io.IOException;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.transport.MessageHandler;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.Protocol;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.GrpcMessage;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc.TransportServiceImplBase;

public class GrpcMessageReceiver implements MessageReceiver {

    private final int port;
    private final Server server;

    private MessageHandler handler;

    public GrpcMessageReceiver(int port) {
        this.port = port;
        this.server = ServerBuilder.forPort(this.port).addService(new TransportService()).build();
    }

    @Override
    public void start() {

        try {
            server.start();
            System.out.println("[GRPC] Listening on port: " + port);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override 
    public void stop() {
        server.shutdown();
    }

    @Override 
    public void setMessageHandler(MessageHandler handler) {
        this.handler = handler;
    }

    public class TransportService extends TransportServiceImplBase {

        @Override
        public void transmitMessage(GrpcMessage request, StreamObserver<GrpcMessage> responseObserver) {
            try {
                // Converts MessageProto to Message
                Message javaMessage = GrpcMapper.toJava(request);

                if (handler != null && javaMessage != null) {

                    Message javaResponse = handler.handle(javaMessage, Protocol.GRPC);

                    if (javaResponse != null) {
                        // converts Message to MessageProto
                        GrpcMessage grpcResponse = GrpcMapper.toGrpc(javaResponse);
                        responseObserver.onNext(grpcResponse);
                    } else {
                        // empty response
                        responseObserver.onNext(GrpcMessage.getDefaultInstance());
                    }
                } else {
                    responseObserver.onNext(GrpcMessage.getDefaultInstance());
                }
                responseObserver.onCompleted();

            } catch (Exception e) {
                e.printStackTrace();
                responseObserver.onError(e);
            }
        }
        
    }
}


