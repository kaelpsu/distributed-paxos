package ufrn.kael.distributedPaxos.common.message;

import java.util.UUID;

import ufrn.kael.distributedPaxos.paxos.actors.ProposalId;

public interface PaxosCommand extends Message {
    
    record Prepare(String messageId, String senderId, String targetId, ProposalId proposalId) implements PaxosCommand {}
    
    record Promise(String messageId, String senderId, String targetId, ProposalId proposalId, ProposalId highestAcceptedId, Transaction acceptedValue) implements PaxosCommand {}
    
    record Accept(String messageId, String senderId, String targetId, ProposalId proposalId, Transaction value) implements PaxosCommand {}
    
    record Accepted(String messageId, String senderId, String targetId, ProposalId proposalId, Transaction value) implements PaxosCommand {}

    static String generateId() {
        return UUID.randomUUID().toString();
    }
}
