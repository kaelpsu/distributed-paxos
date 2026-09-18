package ufrn.kael.distributedPaxos.common.transport.tcp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.JsonSerializer;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

public class TcpMessageSender extends AbstractMessageSender {

    public TcpMessageSender(Map<String, InetSocketAddress> clusterTopology) {
        super(clusterTopology);
    }

    @Override
    protected void transmit(Message message, InetSocketAddress address) throws Exception {
        try (Socket socket = new Socket()) {

            socket.connect(address, 1500);
            
            Serializer serializer = new JsonSerializer();

            byte[] payload = serializer.serialize(message);
            
            OutputStream out = socket.getOutputStream();
            out.write(payload);
            out.flush();
            socket.shutdownOutput(); // eof flag so that the serializer knows the message is over
        } catch (IOException e) {
            throw new RuntimeException("NETWORK_ERROR: Unreachable target node (" + address.getHostName() + ":" + address.getPort() + ")", e);
        }
    }

}