package ufrn.kael.distributedPaxos.common.transport;

public interface MessageReceiver {

    void start();

    void stop();

    void setMessageHandler(MessageHandler handler); // allows each node to set their own way of processing messages
}