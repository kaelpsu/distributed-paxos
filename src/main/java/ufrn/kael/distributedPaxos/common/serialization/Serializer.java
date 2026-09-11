package ufrn.kael.distributedPaxos.common.serialization;

import java.io.InputStream;

import ufrn.kael.distributedPaxos.common.message.Message;

public interface Serializer {
    byte[] serialize(Message message);

    Message deserialize(InputStream inputStream);
    
}
