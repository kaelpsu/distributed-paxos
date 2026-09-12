package ufrn.kael.distributedPaxos.stateful.state;

import ufrn.kael.distributedPaxos.common.message.Transaction;
import ufrn.kael.distributedPaxos.common.transport.Protocol;

public interface StateMachine {

    void apply(Transaction transaction, Protocol protocol);

}
