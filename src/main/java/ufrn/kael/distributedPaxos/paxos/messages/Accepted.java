package ufrn.kael.distributedPaxos.paxos.messages;

public record Accepted<T> (ProposalId proposalId, T value) {}
