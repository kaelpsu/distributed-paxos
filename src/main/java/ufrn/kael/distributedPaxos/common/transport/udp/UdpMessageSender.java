package ufrn.kael.distributedPaxos.common.transport.udp;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.serialization.JsonSerializer;
import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.Map;

public class UdpMessageSender extends AbstractMessageSender {
    private final DatagramSocket socket;

    public UdpMessageSender(Map<String, InetSocketAddress> clusterTopology) throws Exception {
        super(clusterTopology);
        this.socket = new DatagramSocket(); // reusable socket
    }

    @Override
    protected void transmit(Message message, InetSocketAddress address) throws Exception {
        
        Serializer serializer = new JsonSerializer();

        byte[] payload = serializer.serialize(message);
        
        DatagramPacket packet = new DatagramPacket(
                payload, 
                payload.length, 
                address.getAddress(), 
                address.getPort()
        );
        socket.send(packet);
    }
}