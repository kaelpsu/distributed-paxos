package ufrn.kael.distributedPaxos.paxos.actors;

public record ProposalId(long number, String nodeId) implements Comparable<ProposalId> {

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
        return this.nodeId.compareTo(other.nodeId);
    }
    
}
