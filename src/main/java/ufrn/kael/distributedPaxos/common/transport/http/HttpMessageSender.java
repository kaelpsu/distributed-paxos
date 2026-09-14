package ufrn.kael.distributedPaxos.common.transport.http;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.JsonSerializer;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

public class HttpMessageSender extends AbstractMessageSender {

    public HttpMessageSender(Map<String, InetSocketAddress> clusterTopology) {
        super(clusterTopology);
    }

    @Override
    protected void transmit(Message message, InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            
            Serializer serializer = new JsonSerializer();

            byte[] payload = serializer.serialize(message);
            
            OutputStream out = socket.getOutputStream();
            
            String httpHeader = "POST / HTTP/1.1\r\n" +
                                "Host: " + address.getHostString() + ":" + address.getPort() + "\r\n" +
                                "Content-Type: application/json\r\n" +
                                "Content-Length: " + payload.length + "\r\n" +
                                "Connection: close\r\n\r\n";
            
            out.write(httpHeader.getBytes());
            out.write(payload); // sends json as content
            out.flush();
        }
    }
}