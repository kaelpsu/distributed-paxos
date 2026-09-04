package ufrn.kael.distributedPaxos.common.serialization;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;
import ufrn.kael.distributedPaxos.common.message.Message;

public class JsonSerializer implements Serializer {

    private final ObjectMapper mapper;

    public JsonSerializer() {
        this.mapper = JsonMapper.builder().findAndAddModules().build();
    }

    public byte[] serialize(Message<?> message) {
        return mapper.writeValueAsBytes(message);
    }

    public <T> Message<T> deserialize(byte[] data, Class<T> payloadType) {
        return mapper.readValue(
            data, mapper.getTypeFactory()
            .constructParametricType(Message.class, payloadType));
    }
    
}
