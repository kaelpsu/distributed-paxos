package ufrn.kael.distributedPaxos.gateway;

import ufrn.kael.distributedPaxos.common.Node;
import ufrn.kael.distributedPaxos.common.HeartbeatRegistry;
import ufrn.kael.distributedPaxos.common.loadbalancing.RoundRobinBalancer;
import ufrn.kael.distributedPaxos.common.message.ApplicationCommand;
import ufrn.kael.distributedPaxos.common.message.ApplicationResponse;
import ufrn.kael.distributedPaxos.common.message.Heartbeat;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.message.Transaction;
import ufrn.kael.distributedPaxos.common.transport.MessageReceiver;
import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ApiGateway extends Node {

    private final RoundRobinBalancer businessLoadBalancer;
    private final HeartbeatRegistry registry; // keeps track of alive nodes (those who sent heartbeat)

   // map for locking thread execution until a response is generated
    private final Map<String, CompletableFuture<ApplicationResponse>> pendingRequests = new ConcurrentHashMap<>();
    
    public ApiGateway(
            String nodeId,
            List<MessageReceiver> internalReceivers,
            NetworkRouter internalRouter
    ) {
        super(nodeId, internalReceivers, internalRouter);

        // internally, the gateway only communicates with business nodes
        this.businessLoadBalancer = new RoundRobinBalancer();
        this.registry = new HeartbeatRegistry();

        for (MessageReceiver receiver : this.receivers) {
            receiver.setMessageHandler(this::processMessage);
        }
    }

    // network receiver thread calls this function as its handler
    private Message processMessage(Message message, Protocol protocol) {

        switch (message) {
            case ApplicationCommand command -> {
                return handleApplicationCommand(command, protocol);
            }

            case ApplicationResponse response -> handleApplicationResponse(response, protocol);
            
            case Heartbeat heartbeat -> handleHeartbeat(heartbeat);

            default ->
                    System.out.println(
                            "[Gateway] Unsupported message: "
                                    + message.getClass()
                                    .getSimpleName()
                    );
        }

        return null;
    }

    private ApplicationResponse handleApplicationCommand(ApplicationCommand command, Protocol protocol) {
        
        String messageId = UUID.randomUUID().toString();

        Transaction transaction = command.transaction();

        // allow killing business nodes for testing resilience
        if ("SHUTDOWN".equals(transaction.operation())) {
            String targetNode = transaction.parameters().get("nodeId");

            ApplicationCommand killCommand = new ApplicationCommand(
                    messageId,
                    this.nodeId,
                    targetNode,
                    this.nodeId,
                    transaction
            );

            router.send(killCommand, protocol);

            return new ApplicationResponse(
                    messageId, 
                    this.nodeId, 
                    "CLIENT", 
                    this.nodeId, 
                    true,
                    "Kill order sent no node: " + targetNode
            );
        }

        List<String> activeNodes = registry.getActiveBusinessNodes();

        if ("GET_ALIVE_NODES".equals(command.transaction().operation())) {
            
            String joinedNodes = String.join(",", activeNodes);

            return new ApplicationResponse(
                    command.messageId(),
                    this.nodeId,
                    command.originId(),
                    command.originId(),
                    true,
                    "STREAM_PAYLOAD:" + joinedNodes
            );
        }

        if (activeNodes.isEmpty()) {
            System.err.println("THERE ARE NOT AVAILABLE BUSINESS NODES");

            return new ApplicationResponse(
                    messageId, 
                    this.nodeId, 
                    "CLIENT", 
                    this.nodeId, 
                    false,
                    "[ERROR] There are no available business nodes."
            );
        }

        String targetBusinessNode = businessLoadBalancer.getNextNode(activeNodes);
        
        ApplicationCommand redirectedCommand = new ApplicationCommand(
                messageId,
                this.nodeId,           // senderId
                targetBusinessNode,    // targetId
                this.nodeId,           // originId
                transaction
        );

        // placeholder for a future response
        CompletableFuture<ApplicationResponse> future = new CompletableFuture<>();

        pendingRequests.put(messageId, future);

        try {
            router.send(redirectedCommand, protocol);
        } catch (RuntimeException e) {
            System.err.println(e.getMessage());

            pendingRequests.remove(messageId);
            return new ApplicationResponse(
                    messageId, 
                    this.nodeId, 
                    "CLIENT", 
                    this.nodeId, 
                    false,
                    "[ERROR] Routing failed: " + e.getMessage()
            );
        }

        try {
            // forcing thread to wait at most 5 seconds for the response and return its value
            ApplicationResponse response = future.get(5, TimeUnit.SECONDS);

            if (!response.success() && response.message().startsWith("ROUTING_ERROR")) {

                System.err.println(response.message());

                // instantly closes socket in case of routing errors
                return new ApplicationResponse(
                    messageId, 
                    this.nodeId, 
                    "CLIENT", 
                    this.nodeId, 
                    false,
                    "[ERROR] Routing failed: " + response.message()
                );
            }

            return response;


        } catch (Exception e) {
            System.err.println(e.getMessage());
            
            pendingRequests.remove(messageId);
            return new ApplicationResponse(
                messageId, 
                this.nodeId, 
                "CLIENT", 
                this.nodeId, 
                false,
                "[ERROR] Transaction failed or timed out during consensus: " + e.getMessage()
            );
        }
    }

    private void handleApplicationResponse(ApplicationResponse response, Protocol protocol) {
        CompletableFuture<ApplicationResponse> future = pendingRequests.remove(response.messageId());

        if (future != null) {
            // indicates that the response arrived, and that the thread can continue
            future.complete(response);
        }

    }

    private void handleHeartbeat(Heartbeat heartbeat) {
        this.registry.registerOrUpdateNode(heartbeat.senderId());
    }

    @Override
    public void start() {
        for (MessageReceiver receiver : receivers) {
            receiver.start();
        }
        System.out.println("[" + nodeId + "] API Gateway started.");

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
        System.out.println("[" + nodeId + "] API Gateway stopped.");
    }
}