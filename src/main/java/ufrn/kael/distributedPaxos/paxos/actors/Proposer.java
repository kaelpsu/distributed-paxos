package ufrn.kael.distributedPaxos.paxos.actors;

import java.util.HashMap;
import java.util.Map;

import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand.Prepare;
import ufrn.kael.distributedPaxos.common.message.Transaction;

public class Proposer {
    
    private final String nodeId;
    private final int quorumSize;
    private final ProposalId currentProposalId;
    
    // maps nodeIds to promises received from acceptors (avoids duplicate counting)
    private final Map<String, PaxosCommand.Promise> promises = new HashMap<>(); 

    private ProposalId highestAcceptedIdSeen;
    private Transaction valueToPropose;

    private boolean acceptSent;


    public Proposer(String nodeId, int quorumSize, ProposalId currentProposalId, Transaction valueToPropose) {
        if (quorumSize <= 0) {
            throw new IllegalArgumentException("Quorum size must be greater than zero.");
        }

        this.nodeId = nodeId;
        this.quorumSize = quorumSize;
        this.currentProposalId = currentProposalId;
        this.valueToPropose = valueToPropose;
    }

    public Prepare createPrepare() {
        return new PaxosCommand.Prepare(
                PaxosCommand.generateId(),
                nodeId,
                "*", // broadcast id
                currentProposalId
        );
    }

    public PaxosCommand.Accept receivePromise(PaxosCommand.Promise promise) {
        if (!promise.proposalId().equals(currentProposalId) || acceptSent) {
            return null;
        }

        if (promise.highestAcceptedId() != null) {
            if (highestAcceptedIdSeen == null || promise.highestAcceptedId().compareTo(highestAcceptedIdSeen) > 0) {

                highestAcceptedIdSeen = promise.highestAcceptedId();
                valueToPropose = promise.acceptedValue();

            }
        }

        promises.put(promise.senderId(), promise);
        
        if (promises.size() >= quorumSize) {
            acceptSent = true;

            return new PaxosCommand.Accept(
                PaxosCommand.generateId(),
                nodeId,
                "*",
                currentProposalId,
                valueToPropose
            );
        }

        return null;
    }
}
