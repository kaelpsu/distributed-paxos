package ufrn.kael.distributedPaxos.paxos.actors;

import ufrn.kael.distributedPaxos.common.message.NodeId;
import ufrn.kael.distributedPaxos.paxos.messages.Accept;
import ufrn.kael.distributedPaxos.paxos.messages.Accepted;
import ufrn.kael.distributedPaxos.paxos.messages.Prepare;
import ufrn.kael.distributedPaxos.paxos.messages.Promise;
import ufrn.kael.distributedPaxos.paxos.messages.ProposalId;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AcceptorTest {

    private final NodeId nodeId =
            new NodeId("db-1");

    @Test
    void deveResponderAoPrimeiroPrepare() {

        Acceptor<String> acceptor = new Acceptor<>();

        ProposalId proposalId = new ProposalId(1, nodeId);

        Promise<String> promise = acceptor.receivePrepare(new Prepare(proposalId));

        assertNotNull(promise);
        assertEquals(proposalId, promise.proposalId());
        assertNull(promise.acceptedProposalId());
        assertNull(promise.acceptedValue());
    }

    @Test
    void deveAceitarValorComPropostaValida() {

        Acceptor<String> acceptor = new Acceptor<>();

        ProposalId proposalId = new ProposalId(1, nodeId);

        acceptor.receivePrepare(new Prepare(proposalId));

        Accepted<String> accepted = acceptor.receiveAccept(new Accept<>(proposalId, "valor-A"));

        assertNotNull(accepted);
        assertEquals(proposalId, accepted.proposalId());
        assertEquals("valor-A", accepted.value());
    }

    @Test
    void deveRejeitarPrepareComPropostaMenor() {

        Acceptor<String> acceptor = new Acceptor<>();

        ProposalId higher = new ProposalId(2, nodeId);

        ProposalId lower = new ProposalId(1, nodeId);

        acceptor.receivePrepare(new Prepare(higher));

        Promise<String> promise = acceptor.receivePrepare(new Prepare(lower));

        assertNull(promise);
    }
}