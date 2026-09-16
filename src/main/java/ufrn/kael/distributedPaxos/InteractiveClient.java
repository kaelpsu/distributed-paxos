package ufrn.kael.distributedPaxos;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.JsonSerializer;
import ufrn.kael.distributedPaxos.common.transport.grpc.GrpcMapper;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.GrpcMessage;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc;
import ufrn.kael.distributedPaxos.common.transport.grpc.generated.TransportServiceGrpc.TransportServiceBlockingStub;

public class InteractiveClient {

    private static final String HOST = "localhost";
    private static final int TCP_PORT = 8080;
    private static final int UDP_PORT = 9090;
    private static final int GRPC_PORT = 50051;

    public static void main(String[] args) {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("=== MULTIPROTOCOL PAXOS CLIENT ===");

            while (true) {
            
                System.out.println("Choose the communication protocol:");
                System.out.println("1. TCP");
                System.out.println("2. UDP");
                System.out.println("3. HTTP");
                System.out.println("4. GRPC");
                System.out.print("Your choice (1/2/3/4): ");
                int choice = Integer.parseInt(scanner.nextLine());

                System.out.print("Type the key to be saved: ");
                String key = scanner.nextLine();

                System.out.print("Type the value: ");
                String value = scanner.nextLine();

                String messageId = "req-" + UUID.randomUUID().toString().substring(0, 8);

                // \n is the eof, so the payload must be in a single line
                String jsonPayload = """
                {"type":"COMMAND","messageId":"%s","senderId":"INTERACTIVE-CLIENT","targetId":"gateway-1","originId":"INTERACTIVE-CLIENT","transaction":{"operation":"SET","parameters":{"key":"%s","value":"%s"}}}
                """.formatted(messageId, key, value);

                System.out.println("\n[Sending request... Wait for consensus]");
                long startTime = System.currentTimeMillis();

                try {
                    switch (choice) {
                        case 1 -> sendTcp(jsonPayload);
                        case 2 -> sendUdp(jsonPayload);
                        case 3 -> sendHttp(jsonPayload);
                        case 4 -> sendGrpc(jsonPayload);
                        default -> System.err.println("Invalid option.");
                    }
                } catch (Exception e) {
                    System.err.println("Communication failed: " + e.getMessage());
                }

                long elapsedTime = System.currentTimeMillis() - startTime;
                System.out.println("\nTotal elapsed time: " + elapsedTime + "ms");
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }
    }

    private static void sendTcp(String payload) throws Exception {
        try (Socket socket = new Socket(HOST, TCP_PORT)) {
            OutputStream out = socket.getOutputStream();
            out.write(payload.getBytes(StandardCharsets.UTF_8));
            out.flush();

            InputStream in = socket.getInputStream();
            String response = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            
            System.out.println("=== TCP RESPONSE ===");
            System.out.println(response);
        }
    }

    private static void sendUdp(String payload) throws Exception {
        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(5000); // 5 seconds timeout

            byte[] sendData = payload.getBytes(StandardCharsets.UTF_8);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, InetAddress.getByName(HOST), UDP_PORT);
            socket.send(sendPacket);

            byte[] receiveData = new byte[8192];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            
            try {
                socket.receive(receivePacket);
                String response = new String(receivePacket.getData(), 0, receivePacket.getLength(), StandardCharsets.UTF_8);
                System.out.println("=== UDP RESPONSE ===");
                System.out.println(response);
            } catch (SocketTimeoutException e) {
                System.err.println("UDP Timeout! Packet was lost or consensus was not reach.");
            }
        }
    }

    private static void sendHttp(String payload) throws Exception {
        try (Socket socket = new Socket(HOST, TCP_PORT)) {

            OutputStream out = socket.getOutputStream();

            byte[] payloadBytes = payload.getBytes(StandardCharsets.UTF_8);

            String httpHeader =
                    "POST / HTTP/1.1\r\n"
                    + "Host: " + HOST + ":" + TCP_PORT + "\r\n"
                    + "Content-Type: application/json\r\n"
                    + "Content-Length: " + payloadBytes.length + "\r\n"
                    + "Connection: close\r\n"
                    + "\r\n";

            out.write(httpHeader.getBytes(StandardCharsets.UTF_8));

            out.write(payloadBytes);

            out.flush();

            InputStream in = socket.getInputStream();

            String response = new String(
                    in.readAllBytes(),
                    StandardCharsets.UTF_8
            );

            System.out.println("=== HTTP RESPONSE ===");
            System.out.println(response);
        }
    }

    private static void sendGrpc(String payload) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(HOST, GRPC_PORT)
                .usePlaintext()
                .build();

        TransportServiceBlockingStub stub = TransportServiceGrpc.newBlockingStub(channel);

        Serializer serializer = new JsonSerializer();
        Message message = serializer.deserialize(new ByteArrayInputStream(payload.getBytes()));

        GrpcMessage request = GrpcMapper.toGrpc(message);

        var response = stub.withDeadlineAfter(5, TimeUnit.SECONDS).transmitMessage(request);

        System.out.println("=== GRPC RESPONSE ===");
        System.out.println(response);


    }
}