package ufrn.kael.distributedPaxos.common.serialization;

import ufrn.kael.distributedPaxos.common.dto.Command;
import ufrn.kael.distributedPaxos.common.message.Message;
import ufrn.kael.distributedPaxos.common.message.MessageType;
import ufrn.kael.distributedPaxos.common.message.NodeId;

import org.junit.jupiter.api.Test;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonSerializerTest {

    private final JsonSerializer serializer = new JsonSerializer();

    @Test
    void deveSerializarEDesserializarMensagem() {

        NodeId sender = new NodeId("business-1");
        NodeId receiver = new NodeId("db-1");

        Command command = new Command(
                "SET",
                Map.of(
                        "key", "account:10",
                        "value", "500"
                )
        );

        Message<Command> original = Message.create(
                sender,
                receiver,
                MessageType.COMMAND,
                command
        );

        byte[] data = serializer.serialize(original);

        assertNotNull(data);
        assertTrue(data.length > 0);

        Message<Command> restored = serializer.deserialize(data, Command.class);

        assertEquals(original.id(), restored.id());
        assertEquals(original.sender(), restored.sender());
        assertEquals(original.receiver(), restored.receiver());
        assertEquals(original.type(), restored.type());
        assertEquals(original.payload(), restored.payload());
    }

    @Test
    void serializacaoDeveProduzirJson() {

        Command command = new Command(
                "SET",
                Map.of("key", "x")
        );

        Message<Command> message = Message.create(
                new NodeId("business-1"),
                new NodeId("db-1"),
                MessageType.COMMAND,
                command
        );

        byte[] data = serializer.serialize(message);

        String json = new String(
                data,
                StandardCharsets.UTF_8
        );

        assertTrue(json.startsWith("{"));
        assertTrue(json.contains("\"type\":\"COMMAND\""));
        assertTrue(json.contains("\"operation\":\"SET\""));
    }
}
