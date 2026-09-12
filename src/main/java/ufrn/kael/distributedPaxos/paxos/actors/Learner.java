package ufrn.kael.distributedPaxos.paxos.actors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.transport.Protocol;
import ufrn.kael.distributedPaxos.stateful.state.StateMachine;

public class Learner {

    private final int quorumSize;
    private final StateMachine stateMachine;

    private final Map<ProposalId, Set<String>> acceptances = new HashMap<>();

    private boolean decided;

    public Learner(int quorumSize, StateMachine stateMachine) {
        if (quorumSize <= 0) {
            throw new IllegalArgumentException("Quorum size must be greater than zero.");
        }

        this.quorumSize = quorumSize;
        this.stateMachine = stateMachine;
    }

    public void receiveAccepted(PaxosCommand.Accepted accepted, Protocol protocol) {
        if (decided) {
            return;
        }

        ProposalId proposalId = accepted.proposalId();

        // if the proposalId is not in the map, create a new set for it
        Set<String> acceptors = acceptances.computeIfAbsent(proposalId, k -> new HashSet<>());

        acceptors.add(accepted.senderId());

        if (acceptors.size() >= quorumSize) {
            decided = true;
            stateMachine.apply(accepted.value(), protocol);
        }
    }

    public boolean hasDecided() {
        return decided;
    }

}
