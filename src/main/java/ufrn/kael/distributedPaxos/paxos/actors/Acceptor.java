package ufrn.kael.distributedPaxos.paxos.actors;

import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.Transaction;

public class Acceptor {
    private final String nodeId;
    private ProposalId promisedId;
    private ProposalId acceptedProposalId;
    private Transaction acceptedValue;

    public Acceptor(String nodeId) {
        this.nodeId = nodeId;
    }

    public PaxosCommand.Promise receivePrepare(PaxosCommand.Prepare prepare) {
        ProposalId proposalId = prepare.proposalId();
        if (promisedId == null || proposalId.compareTo(promisedId) > 0) {
            promisedId = proposalId;
            return new PaxosCommand.Promise(
                    PaxosCommand.generateId(),
                    this.nodeId,
                    prepare.senderId(),
                    proposalId,
                    acceptedProposalId,
                    acceptedValue
            );
        }
        
        return null;
    }

    public PaxosCommand.Accepted receiveAccept(PaxosCommand.Accept accept) {
        ProposalId proposalId = accept.proposalId();
        
        if (promisedId == null || proposalId.compareTo(promisedId) >= 0) {
            promisedId = proposalId;
            acceptedProposalId = proposalId;
            acceptedValue = accept.value();

            return new PaxosCommand.Accepted(
                    PaxosCommand.generateId(),
                    this.nodeId,
                    "*",
                    proposalId,
                    acceptedValue
            );
        }
        return null;
    }

}
