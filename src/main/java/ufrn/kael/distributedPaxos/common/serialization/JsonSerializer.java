package ufrn.kael.distributedPaxos.common.serialization;

import java.io.InputStream;

import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import ufrn.kael.distributedPaxos.common.message.Message;

public class JsonSerializer implements Serializer {

    private final ObjectMapper mapper;

    public JsonSerializer() {
        this.mapper = JsonMapper.builder()
                        .findAndAddModules()
                        .disable(StreamReadFeature.AUTO_CLOSE_SOURCE) // prevents jackson from closing connection right after deserialization (we need it for responses)
                        .build();
    }

    public byte[] serialize(Message message) {
        return mapper.writeValueAsBytes(message);
    }

    public Message deserialize(InputStream inputStream) {
        return mapper.readValue(inputStream, Message.class);
    }
}
