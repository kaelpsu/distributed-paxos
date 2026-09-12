package ufrn.kael.distributedPaxos.stateful.state;

import java.util.function.BiConsumer;

import ufrn.kael.distributedPaxos.common.message.ApplicationResponse;
import ufrn.kael.distributedPaxos.common.message.Transaction;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

public class DatabaseStateMachine implements StateMachine {

    private final InMemoryDatabase database;
    private final BiConsumer<ApplicationResponse, Protocol> onTransactionApplied;

    public DatabaseStateMachine(InMemoryDatabase database, BiConsumer<ApplicationResponse, Protocol> onTransactionApplied) {
        this.database = database;
        this.onTransactionApplied = onTransactionApplied;
    }

    @Override
    public void apply(Transaction transaction, Protocol protocol) {
        boolean success = true;
        String resultMessage = "OK";

        try {
            switch (transaction.operation()) {
                case "SET" -> {
                    String key = transaction.parameters().get("key");
                    String value = transaction.parameters().get("value");
                    database.set(key, value);
                }
                case "DELETE" -> {
                    String key = transaction.parameters().get("key");
                    database.delete(key);
                }
                default -> {
                    success = false;
                    resultMessage = "Unknown transaction: " + transaction.operation();
                }
            }
        } catch (Exception e) {
            success = false;
            resultMessage = e.getMessage();
        }

        // recovers metadata injected by the DatabaseNode
        String messageId = transaction.parameters().get("_messageId");
        String originId = transaction.parameters().get("_originId");
        String replyTo = transaction.parameters().get("_replyTo");
        String nodeId = transaction.parameters().get("_nodeId");
    
        if (messageId != null && replyTo != null) {
            ApplicationResponse response = new ApplicationResponse(
                messageId,
                nodeId,
                replyTo,
                originId,
                success,
                resultMessage 
            );
            onTransactionApplied.accept(response, protocol);
        }
    }

    public InMemoryDatabase database() {
        return database;
    }
}