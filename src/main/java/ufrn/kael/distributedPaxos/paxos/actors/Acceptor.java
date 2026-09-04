package ufrn.kael.distributedPaxos.paxos.actors;

import ufrn.kael.distributedPaxos.paxos.messages.Accept;
import ufrn.kael.distributedPaxos.paxos.messages.Accepted;
import ufrn.kael.distributedPaxos.paxos.messages.Prepare;
import ufrn.kael.distributedPaxos.paxos.messages.Promise;
import ufrn.kael.distributedPaxos.paxos.messages.ProposalId;

public class Acceptor<T> {
    private ProposalId promisedId;
    private ProposalId acceptedProposalId;
    private T acceptedValue;

    public Promise<T> receivePrepare(Prepare prepare) {
        ProposalId proposalId = prepare.proposalId();
        if (promisedId == null || proposalId.compareTo(promisedId) > 0) {
            promisedId = proposalId;
            return new Promise<>(proposalId, acceptedProposalId, acceptedValue);
        } 
        
        return null;
    }

    public Accepted<T> receiveAccept(Accept<T> accept) {
        ProposalId proposalId = accept.proposalId();
        T value = accept.value();
        if (promisedId != null && proposalId.compareTo(promisedId) == 0) {
            acceptedProposalId = proposalId;
            acceptedValue = value;
            return new Accepted<>(proposalId, value);
        }
        return null;
    }

}
