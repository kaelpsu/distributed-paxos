package ufrn.kael.distributedPaxos.paxos;

import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand.Accept;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand.Accepted;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand.Prepare;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand.Promise;
import ufrn.kael.distributedPaxos.common.message.Transaction;
import ufrn.kael.distributedPaxos.common.transport.MessageHandler;
import ufrn.kael.distributedPaxos.common.transport.NetworkRouter;
import ufrn.kael.distributedPaxos.common.transport.Protocol;
import ufrn.kael.distributedPaxos.paxos.actors.Acceptor;
import ufrn.kael.distributedPaxos.paxos.actors.Learner;
import ufrn.kael.distributedPaxos.paxos.actors.ProposalId;
import ufrn.kael.distributedPaxos.paxos.actors.Proposer;
import ufrn.kael.distributedPaxos.stateful.state.StateMachine;

public class PaxosEngine implements MessageHandler {

    private final String nodeId;
    private final int quorumSize;
    private final Acceptor acceptor;
    private final Learner learner;
    private final NetworkRouter router;
    
    private Proposer proposer; // each proposal instantiates a new proposer

    private long proposalNumber;

    public PaxosEngine(String nodeId, int quorumSize, StateMachine stateMachine, NetworkRouter router) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("Node ID cannot be null or blank.");
        }

        if (quorumSize <= 0) {
            throw new IllegalArgumentException("Quorum size must be greater than zero.");
        }

        this.nodeId = nodeId;
        this.quorumSize = quorumSize;
        this.acceptor = new Acceptor(nodeId);
        this.learner = new Learner(quorumSize, stateMachine);
        this.router = router;
    }

    public synchronized void proposeValue(Transaction value, Protocol protocol) {
        
        if (hasDecided()) {
            throw new IllegalStateException("ALREADY_DECIDED: Consensus has already been reached and wont change.");
        }

        if (this.proposer != null && !hasDecided()) {
            throw new IllegalStateException("BUSY: A Paxos iteration is already happening. Try again later.");
        }
        
        ProposalId proposalId = nextProposalId();
        
        this.proposer = new Proposer(nodeId, quorumSize, proposalId, value);

        Prepare prepare = proposer.createPrepare();

        router.send(prepare, protocol);
    }

    private ProposalId nextProposalId() {
        return new ProposalId(
                ++proposalNumber,
                nodeId
        );
    }

    @Override 
    public synchronized Message handle(Message message, Protocol protocol) {
        switch (message) {
            case PaxosCommand.Prepare prepare -> handlePrepare(prepare, protocol);

            case PaxosCommand.Promise promise -> handlePromise(promise, protocol);

            case PaxosCommand.Accept accept -> handleAccept(accept, protocol);

            case PaxosCommand.Accepted accepted -> handleAccepted(accepted, protocol);

            default -> throw new IllegalArgumentException("[PAXOS HANDLER]: Unknown message type: " + message.getClass().getSimpleName());
        }

        return null;
    }

    private void handlePrepare(PaxosCommand.Prepare prepare, Protocol protocol) {

        Promise promise = acceptor.receivePrepare(prepare);
        
        if (promise != null) {
            router.send(promise, protocol);
        }
    }

    private void handlePromise(PaxosCommand.Promise promise, Protocol protocol) {

        if (proposer == null) {
            throw new IllegalStateException("[PAXOS ENGINE]: Received a promise without an active proposer.");
        }
        
        Accept accept = proposer.receivePromise(promise);
        
        if (accept != null) {
            router.send(accept, protocol);
        }
    }

    private void handleAccept(PaxosCommand.Accept accept, Protocol protocol) {

        Accepted accepted = acceptor.receiveAccept(accept);

        if (accepted != null) {
            router.send(accepted, protocol);
        }
    }

    private void handleAccepted(PaxosCommand.Accepted accepted, Protocol protocol) {
        learner.receiveAccepted(accepted, protocol);
    }

    public boolean hasDecided() {
        return learner.hasDecided();
    }

    public String getString() {
        return nodeId;
    }    
}
