package ufrn.kael.distributedPaxos.paxos.messages;

public record Accept<T> (ProposalId proposalId, T value) {}