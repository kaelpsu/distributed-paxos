package ufrn.kael.distributedPaxos.common.serialization;

import ufrn.kael.distributedPaxos.common.message.Message;

public interface Serializer {
    byte[] serialize(Message<?> message);

    <T> Message<T> deserialize(byte[] data, Class<T> payloadType);
    
}
