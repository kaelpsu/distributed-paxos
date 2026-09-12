package ufrn.kael.distributedPaxos.common.transport.tcp;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.MessageHandler;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

public class TcpMessageReceiver implements MessageReceiver {
    private final int port;
    private final Serializer serializer;
    private final ExecutorService threadPool;

    // volatile ensures that changes to this variable are visible to all threads
    private volatile boolean running;

    private ServerSocket serverSocket;
    private MessageHandler handler;

    public TcpMessageReceiver(int port, Serializer serializer, int threadCount) {
        this.port = port;
        this.serializer = serializer;
        this.threadPool = Executors.newFixedThreadPool(threadCount);
    }

    @Override 
    public void start() {
        if (running) return;
        this.running = true;

        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(port)) {
                this.serverSocket = serverSocket;

                System.out.println("[TCP/HTTP] Listening on port: " + port);
    
                while (running) {
                    Socket socket = serverSocket.accept();
                    threadPool.submit(()-> handleClient(socket));
                }
                
            } catch (IOException e) {
                if (running) {
                    e.printStackTrace();
                }
            }

        }, "tcp-listener-" + port).start();

    }

    private void handleClient(Socket socket) {
        String clientAddress = socket.getRemoteSocketAddress().toString();
        System.out.println("[TCP-RECEIVER-" + port + "] --- NEW CONNECTION STARTED FROM: " + clientAddress + " ---");
        
        try (socket) {
            // creates a buffer with the first bytes from the inputsream (useful for checking if its format is http or tcp)
            BufferedInputStream in = new BufferedInputStream(socket.getInputStream());

            OutputStream out = socket.getOutputStream();

            in.mark(1); // saves current buffer position

            int firstByte = in.read();

            in.reset(); // "returns" the read byte to the buffer

            if (firstByte == -1) return; // -1 represents the eof, so we simply close the connection

            if (firstByte == '{' || firstByte == '[') {
                processAsTcp(in, out, clientAddress);
            } else {
                processAsHttp(in, out, clientAddress);
            }

        } catch (Exception e) {
            System.err.println("[TCP-RECEIVER-" + port + "] FATAL ERROR WHILE PROCESSING CLIENT: " + e.getMessage());
            e.printStackTrace();
        }
        
        System.out.println("[TCP-RECEIVER-" + port + "] --- CONNECTION CLOSED: " + clientAddress + " ---\n");
    }

    private void processAsTcp(BufferedInputStream in, OutputStream out, String clientAddress) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String json = reader.readLine(); // gambiarra to use \n as eol marker

        ByteArrayInputStream bais = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));

        System.out.println("[TCP-MODE] Deserializing with Jackson...: " + clientAddress);
        
        Message message = serializer.deserialize(bais);

        System.out.println("[TCP-MODE-" + port + "] Success! Message converted to: " + message.getClass().getSimpleName());
        
        if (handler != null) {
            System.out.println("[TCP-MODE-" + port + "] Redirecting to business layer...");

            Message response = handler.handle(message, Protocol.TCP);
            if (response != null) {
                System.out.println("[TCP-MODE-" + port + "] Generated response of type: " + response.getClass().getSimpleName() + ". Sending it back to client...");
                byte[] responseData = serializer.serialize(response);

                out.write(responseData);
                out.flush();
                System.out.println("[TCP-MODE-" + port + "] Response sent to client with success.");
            } else {
                System.out.println("[TCP-MODE-" + port + "] Handler has returned NULL. Closing current TCP communication.");
            }
        }
    }

    private void processAsHttp(BufferedInputStream in, OutputStream out, String clientAddress) throws Exception {
        System.out.println("[HTTP-MODE] Processing connection from: " + clientAddress);
        
        // using a reader, so we retrieve plain text instead of bytes
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));

        String line;
        int contentLength = 0;

        // extracts http header
        while ((line = reader.readLine()) != null && !line.isEmpty()) {
            if (line.toLowerCase().startsWith("content-length:")) {
                contentLength = Integer.parseInt(line.split(":")[1].trim());
            }
        }

        if (contentLength > 0) {
            char[] bodyChars = new char[contentLength];
            reader.read(bodyChars, 0, contentLength);

            System.out.println("[HTTP-MODE] Deserializing with Jackson...: " + clientAddress);
            
            // converts the bodyChards back to bytes so the deserialize method can read it
            Message message = serializer.deserialize(new ByteArrayInputStream(new String(bodyChars).getBytes()));

            System.out.println("[HTTP-MODE-" + port + "] Success! Message converted to: " + message.getClass().getSimpleName());

            if (handler != null) {
                System.out.println("[HTTP-MODE-" + port + "] Redirecting to business layer...");
                Message response = handler.handle(message, Protocol.HTTP);

                if (response != null) {
                    System.out.println("[HTTP-MODE-" + port + "] Generated response of type: " + response.getClass().getSimpleName() + ". Sending it back to client...");
                    byte[] responseData = serializer.serialize(response);
                    
                    String httpHeaders = "HTTP/1.1 200 OK\r\n" +
                                         "Content-Type: application/json\r\n" +
                                         "Content-Length: " + responseData.length + "\r\n" +
                                         "Connection: close\r\n\r\n"; 

                    out.write(httpHeaders.getBytes());
                    out.write(responseData);
                    out.flush();

                    System.out.println("[HTTP-MODE-" + port + "] Response sent to client with success.");
                }
            } else {
                System.out.println("[HTTP-MODE-" + port + "] Handler has returned NULL. Closing current TCP communication.");
            }
        }
    }

    @Override 
    public void stop() {
        this.running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        threadPool.shutdownNow();
    }

    @Override 
    public void setMessageHandler(MessageHandler handler) {
        this.handler = handler;
    }
}