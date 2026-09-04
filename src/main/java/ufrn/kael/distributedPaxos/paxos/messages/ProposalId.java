package ufrn.kael.distributedPaxos.paxos.messages;

import ufrn.kael.distributedPaxos.common.message.NodeId;

public record ProposalId(long number, NodeId nodeId) implements Comparable<ProposalId> {

    public ProposalId {
        if (number < 0) {
            throw new IllegalArgumentException("Proposal number cannot be negative.");
        }
    }

    @Override
    public int compareTo(ProposalId other) {
        int numberComparison = Long.compare(this.number, other.number);
        if (numberComparison != 0) {
            return numberComparison;
        }
        return this.nodeId.value().compareTo(other.nodeId.value());
    }
    
}
