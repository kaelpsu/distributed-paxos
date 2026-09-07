package ufrn.kael.distributedPaxos.common.serialization;

import org.junit.jupiter.api.Test;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.message.PaxosCommand;
import ufrn.kael.distributedPaxos.common.message.SystemCommand;
import ufrn.kael.distributedPaxos.paxos.actors.ProposalId;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerializerTest {

    private final JsonSerializer serializer = new JsonSerializer();

    @Test
    void shouldSerializeandDeserializePrepare() {

        PaxosCommand.Prepare original =
                new PaxosCommand.Prepare(
                        "msg-1",
                        "db-1",
                        "db-2",
                        new ProposalId(10, "db-1")
                );

        byte[] data = serializer.serialize(original);

        Message restored = serializer.deserialize(data);

        assertInstanceOf(PaxosCommand.Prepare.class, restored);

        assertEquals(original, restored);
    }

    @Test
    void shouldSerializeandDeserializeAccept() {

        SystemCommand command =
                new SystemCommand(
                        "SET",
                        Map.of(
                                "key", "account:10",
                                "value", "500"
                        )
                );

        PaxosCommand.Accept original =
                new PaxosCommand.Accept(
                        "msg-2",
                        "db-1",
                        "db-2",
                        new ProposalId(10, "db-1"),
                        command
                );

        byte[] data = serializer.serialize(original);

        Message restored = serializer.deserialize(data);

        assertInstanceOf(PaxosCommand.Accept.class, restored);

        assertEquals(original, restored);
    }
}