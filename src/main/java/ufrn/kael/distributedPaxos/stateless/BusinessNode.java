package ufrn.kael.distributedPaxos.stateless;

import ufrn.kael.distributedPaxos.common.Node;
import ufrn.kael.distributedPaxos.common.TopologyRegistry;
import ufrn.kael.distributedPaxos.common.loadbalancing.RoundRobinBalancer;
import ufrn.kael.distributedPaxos.common.message.ApplicationCommand;
import ufrn.kael.distributedPaxos.common.message.ApplicationResponse;
import ufrn.kael.distributedPaxos.common.message.Heartbeat;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class BusinessNode extends Node {

    private final RoundRobinBalancer dbLoadBalancer;
    private ScheduledExecutorService heartbeatTimer;

    private final TopologyRegistry registry; // keeps track of alive nodes (those who sent heartbeat)

    List<String> databaseNodes;

    public BusinessNode(
            String nodeId,
            List<MessageReceiver> receivers,
            NetworkRouter router
    ) {
        super(nodeId, receivers, router);

        this.registry = new TopologyRegistry();
        
        if (databaseNodes == null || databaseNodes.isEmpty()) {
            throw new IllegalArgumentException("Business Node requires at least one Database Node to route messages.");
        }
        
        this.dbLoadBalancer = new RoundRobinBalancer();

        for (MessageReceiver receiver : this.receivers) {
            receiver.setMessageHandler(this::processMessage);
        }
    }
  
    private Message processMessage(Message message, Protocol protocol) {

        switch (message) {
            case ApplicationCommand command -> handleApplicationCommand(command, protocol);
        
            case ApplicationResponse response -> handleApplicationResponse(response, protocol);
            
            case Heartbeat heartbeat -> handleHeartbeat(heartbeat);
            default ->
                    System.out.println(
                            "[BIZ} Unsupported message: "
                                    + message.getClass()
                                    .getSimpleName()
                    );
        }

        return null;
    }

    private void handleApplicationCommand(ApplicationCommand command, Protocol protocol) {
        try {
            validateTransaction(command);

            if ("SHUTDOWN".equals(command.transaction().operation())) {
                System.err.println("\n[" + nodeId + "] Received shutdown command! Simulating hardware failure...");
                this.stop();
                return;
            }

            List<String> activeNodes = registry.getActiveDatabaseNodes();

            String targetDbNode = dbLoadBalancer.getNextNode(activeNodes);

            ApplicationCommand forwardedCommand = new ApplicationCommand(
                    command.messageId(),
                    this.nodeId,           
                    targetDbNode,          
                    command.originId(),    
                    command.transaction()
            );

            router.send(forwardedCommand, protocol);

        } catch (IllegalArgumentException e) {
            System.err.println("[" + nodeId + "] Business validation failed: " + e.getMessage());
        }
    }

    private void handleApplicationResponse(ApplicationResponse response, Protocol protocol) {
        // always redirects responses to gateway
        ApplicationResponse forwardedResponse = new ApplicationResponse(
                response.messageId(),
                this.nodeId,
                response.originId(),
                response.originId(),
                response.success(),
                response.message()
        );
        router.send(forwardedResponse, protocol);
    }

    private void handleHeartbeat(Heartbeat heartbeat) {
        this.registry.registerOrUpdateNode(heartbeat.senderId());
    }

    private void validateTransaction(ApplicationCommand command) {
        if (command.transaction().operation() == null) {
            throw new IllegalArgumentException("Operation is missing.");
        }
        
        String op = command.transaction().operation().toUpperCase();
        if (!op.equals("SET") && !op.equals("DELETE") && !op.equals("GET") && !op.equals("SHUTDOWN")) {
            throw new IllegalArgumentException("Invalid business operation: " + op);
        }
    }

    @Override
    public void start() {
        for (MessageReceiver receiver : receivers) {
            receiver.start();
        }
        System.out.println("[" + nodeId + "] Business Node started. Routing to DBs: " + dbLoadBalancer);
    
        startHeartbeat();    

        ScheduledExecutorService removerTimer = Executors.newSingleThreadScheduledExecutor();

        removerTimer.scheduleAtFixedRate(() -> {
            // if a node doesn't send heartbeat after 10 seconds, its considered dead
            registry.removeDeadNodes(10000); 
        }, 5, 5, TimeUnit.SECONDS); // checks every 5 sec
    }

    @Override
    public void stop() {
        for (MessageReceiver receiver : receivers) {
            receiver.stop();
        }
        if (heartbeatTimer != null) {
            heartbeatTimer.shutdownNow();
        }
        System.out.println("[" + nodeId + "] Business Node stopped.");
    }

    private void startHeartbeat() {
        this.heartbeatTimer = Executors.newSingleThreadScheduledExecutor();
        heartbeatTimer.scheduleAtFixedRate(() -> {
            Heartbeat hb = new Heartbeat(
                    java.util.UUID.randomUUID().toString(),
                    this.nodeId,
                    "*",
                    this.nodeId
            );
        
            // always sends heartbeat via udp
            this.router.send(hb, Protocol.UDP); 
            
        }, 0, 3, TimeUnit.SECONDS); // beats every 3 seonds
    }
}