package ufrn.kael.distributedPaxos.paxos.messages;

public record Promise<T>(ProposalId proposalId, ProposalId acceptedProposalId, T acceptedValue) {}
