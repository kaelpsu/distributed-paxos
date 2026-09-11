package ufrn.kael.distributedPaxos.common.transport.tcp;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

public class TcpMessageSender extends AbstractMessageSender {

    public TcpMessageSender(Serializer serializer, Map<String, InetSocketAddress> clusterTopology) {
        super(serializer, clusterTopology);
    }

    @Override
    protected void transmit(byte[] payload, InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket(address.getAddress(), address.getPort())) {
            OutputStream out = socket.getOutputStream();
            out.write(payload);
            out.flush();
            socket.shutdownOutput(); // eof flag so that the serializer knows the message is over
        }
    }

}