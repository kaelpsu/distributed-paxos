package ufrn.kael.distributedPaxos.common.transport.http;

import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

public class HttpMessageSender extends AbstractMessageSender {

    public HttpMessageSender(Serializer serializer, Map<String, InetSocketAddress> clusterTopology) {
        super(serializer, clusterTopology);
    }

    @Override
    protected void transmit(byte[] payload, InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
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