package ufrn.kael.distributedPaxos.common.transport.udp;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.MessageHandler;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UdpMessageReceiver implements MessageReceiver {
    private final int port;
    private final Serializer serializer;
    private final ExecutorService threadPool;

    // volatile ensures that changes to this variable are visible to all threads
    private volatile boolean running;

    private DatagramSocket socket;
    private MessageHandler handler;

    public UdpMessageReceiver(int port, Serializer serializer, int threadCount) {
        this.port = port;
        this.serializer = serializer;
        this.threadPool = Executors.newFixedThreadPool(threadCount);
    }

    @Override
    public void start() {
        if (running) return;
        this.running = true;

        new Thread(() -> {
            try (DatagramSocket datagramSocket = new DatagramSocket(port)) {
                this.socket = datagramSocket;
                
                System.out.println("[UDP] Listening on port: " + port);

                while (running) {
                    byte[] buffer = new byte[8192];
                    DatagramPacket incomingPacket = new DatagramPacket(buffer, buffer.length);
                    
                    socket.receive(incomingPacket);
                    
                    threadPool.submit(() -> handlePacket(incomingPacket));
                }
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }
        }, "udp-listener-" + port).start();
    }

    private void handlePacket(DatagramPacket packet) {
        String clientAddress = packet.getAddress().toString() + ":" + packet.getPort();
        System.out.println("[UDP-RECEIVER-" + port + "] --- NEW DATAGRAM RECEIVED FROM: " + clientAddress + " ---");

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData(), 0, packet.getLength());
            
            System.out.println("[UDP-RECEIVER-" + port + "] Deserializing with Jackson...");
            Message message = serializer.deserialize(bais);
            
            System.out.println("[UDP-RECEIVER-" + port + "] Success! Message converted to: " + message.getClass().getSimpleName());

            if (handler != null) {
                System.out.println("[UDP-RECEIVER-" + port + "] Redirecting to business layer...");
                
                Message response = handler.handle(message, Protocol.UDP);

                if (response != null) {
                    System.out.println("[UDP-RECEIVER-" + port + "] Generated response of type: " + response.getClass().getSimpleName() + ". Sending it back to client...");
                    byte[] responseData = serializer.serialize(response);
                    
                    DatagramPacket responsePacket = new DatagramPacket(
                            responseData, 
                            responseData.length, 
                            packet.getAddress(), 
                            packet.getPort()
                    );

                    socket.send(responsePacket);
                    System.out.println("[UDP-RECEIVER-" + port + "] Response sent to client with success.");
                } else {
                    System.out.println("[UDP-RECEIVER-" + port + "] Handler has returned NULL. Closing current TCP communication.");
                }
            } else {
                System.err.println("[UDP-RECEIVER-" + port + "] WARNING: No handlers set for this port!");
            }
        } catch (Exception e) {
            System.err.println("[UDP-RECEIVER-" + port + "] FATAL ERROR WHILE PROCESSING DATAGRAM: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("[UDP-RECEIVER-" + port + "] --- CONNECTION CLOSED: " + clientAddress + " ---\n");
    }

    @Override
    public void stop() {
        this.running = false;
        if (socket != null && !socket.isClosed()) {
            socket.close();
        }
        threadPool.shutdownNow();
    }

    @Override 
    public void setMessageHandler(MessageHandler handler) {
        this.handler = handler;
    }
}