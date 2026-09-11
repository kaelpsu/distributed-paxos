package ufrn.kael.distributedPaxos.common.transport;

import ufrn.kael.distributedPaxos.common.message.Message;

@FunctionalInterface
public interface MessageHandler {

    Message handle(Message message, Protocol protocol);
    
}
