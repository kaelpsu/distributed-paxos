package ufrn.kael.distributedPaxos.common.serialization;

import ufrn.kael.distributedPaxos.common.message.Message;

public interface Serializer {
    byte[] serialize(Message message);

    Message deserialize(byte[] data);
    
}
