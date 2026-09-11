package ufrn.kael.distributedPaxos.paxos.actors;

import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.Transaction;

public class Acceptor {
    private ProposalId promisedId;
    private ProposalId acceptedProposalId;
    private Transaction acceptedValue;

    public PaxosCommand.Promise receivePrepare(PaxosCommand.Prepare prepare) {
        ProposalId proposalId = prepare.proposalId();
        if (promisedId == null || proposalId.compareTo(promisedId) > 0) {
            promisedId = proposalId;
            return new PaxosCommand.Promise(
                    PaxosCommand.generateId(),
                    prepare.targetId(),
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
                    accept.targetId(),
                    accept.senderId(),
                    proposalId,
                    acceptedValue
            );
        }
        return null;
    }

}
