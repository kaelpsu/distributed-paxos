package ufrn.kael.distributedPaxos.paxos.actors;

import org.junit.jupiter.api.Test;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.SystemCommand;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AcceptorTest {

    private static final String ACCEPTOR_ID = "db-1";
    private static final String PROPOSER_ID = "db-2";

    private final Acceptor acceptor = new Acceptor();

    @Test
    void shouldRespondToPrepare() {

        ProposalId proposalId = new ProposalId(1, PROPOSER_ID);

        PaxosCommand.Prepare prepare =
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        proposalId
                );

        PaxosCommand.Promise promise = acceptor.receivePrepare(prepare);

        assertNotNull(promise);

        assertEquals(proposalId, promise.proposalId());

        assertEquals(ACCEPTOR_ID, promise.senderId());

        assertEquals(PROPOSER_ID,promise.targetId());

        assertNull(promise.highestAcceptedId());

        assertNull(promise.acceptedValue());
    }

    @Test
    void shouldRejectPrepareWithLowerProposalId() {

        ProposalId higher = new ProposalId(2, PROPOSER_ID);

        ProposalId lower = new ProposalId(1, PROPOSER_ID);

        PaxosCommand.Prepare firstPrepare =
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        higher
                );

        PaxosCommand.Prepare secondPrepare =
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        lower
                );

        // since it has a higher proposal id, the first prepare should be accepted
        assertNotNull(acceptor.receivePrepare(firstPrepare));

        // but not the second one
        assertNull(acceptor.receivePrepare(secondPrepare));
    }

    @Test
    void shouldReturnPreviouslyAcceptedValueInPromise() {

        ProposalId proposalId = new ProposalId(1, PROPOSER_ID);

        SystemCommand value =
                new SystemCommand(
                        "SET",
                        Map.of(
                                "key", "account:10",
                                "value", "500"
                        )
                );

        PaxosCommand.Prepare prepare =
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        proposalId
                );

        PaxosCommand.Promise firstPromise = acceptor.receivePrepare(prepare);

        assertNotNull(firstPromise);

        PaxosCommand.Accept accept =
                new PaxosCommand.Accept(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        proposalId,
                        value
                );

        PaxosCommand.Accepted accepted = acceptor.receiveAccept(accept);

        assertNotNull(accepted);

        PaxosCommand.Promise secondPromise =
                acceptor.receivePrepare(
                        new PaxosCommand.Prepare(
                                PaxosCommand.generateId(),
                                PROPOSER_ID,
                                ACCEPTOR_ID,
                                new ProposalId(2, PROPOSER_ID)
                        )
                );

        assertNotNull(secondPromise);

        // as the proposalId is higher than the previous one, it is accepted
        assertEquals(proposalId, secondPromise.highestAcceptedId());

        // the value, though, is the one that was already accepted on the first round
        assertEquals(value, secondPromise.acceptedValue());
    }

    @Test
    void shouldAcceptValueWithPromisedProposalId() {

        ProposalId proposalId = new ProposalId(1, PROPOSER_ID);

        SystemCommand value =
                new SystemCommand(
                        "SET",
                        Map.of("key", "x")
                );

        PaxosCommand.Prepare prepare =
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        proposalId
                );

        assertNotNull(acceptor.receivePrepare(prepare));

        PaxosCommand.Accept accept =
                new PaxosCommand.Accept(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        proposalId,
                        value
                );

        PaxosCommand.Accepted accepted =
                acceptor.receiveAccept(accept);

        assertNotNull(accepted);

        assertEquals(proposalId, accepted.proposalId());

        assertEquals(value, accepted.value());

        assertEquals(ACCEPTOR_ID, accepted.senderId());

        assertEquals(PROPOSER_ID, accepted.targetId());
    }

    @Test
    void shouldRejectAcceptWithLowerProposalId() {

        ProposalId promised = new ProposalId(2, PROPOSER_ID);

        ProposalId lower = new ProposalId(1, PROPOSER_ID);

        acceptor.receivePrepare(
                new PaxosCommand.Prepare(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        promised
                )
        );

        SystemCommand value =
                new SystemCommand(
                        "SET",
                        Map.of("key", "x")
                );

        PaxosCommand.Accept accept =
                new PaxosCommand.Accept(
                        PaxosCommand.generateId(),
                        PROPOSER_ID,
                        ACCEPTOR_ID,
                        lower,
                        value
                );

        assertNull(acceptor.receiveAccept(accept));
    }
}