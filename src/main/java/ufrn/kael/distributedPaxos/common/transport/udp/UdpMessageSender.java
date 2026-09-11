package ufrn.kael.distributedPaxos.common.transport.udp;

import ufrn.kael.distributedPaxos.common.serialization.Serializer;
import ufrn.kael.distributedPaxos.common.transport.AbstractMessageSender;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.util.Map;

public class UdpMessageSender extends AbstractMessageSender {
    private final DatagramSocket socket;

    public UdpMessageSender(Serializer serializer, Map<String, InetSocketAddress> clusterTopology) throws Exception {
        super(serializer, clusterTopology);
        this.socket = new DatagramSocket(); // reusable socket
    }

    @Override
    protected void transmit(byte[] payload, InetSocketAddress address) throws Exception {
        DatagramPacket packet = new DatagramPacket(
                payload, 
                payload.length, 
                address.getAddress(), 
                address.getPort()
        );
        socket.send(packet);
    }
}