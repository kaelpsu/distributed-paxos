package ufrn.kael.distributedPaxos.common.transport;

import ufrn.kael.distributedPaxos.common.message.Message;

public interface MessageSender {

    void send(Message message);
}
