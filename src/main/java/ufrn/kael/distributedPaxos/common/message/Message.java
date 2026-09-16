package ufrn.kael.distributedPaxos.common.message;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import ufrn.kael.distributedPaxos.common.message.PaxosCommand.*;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type"
)
@JsonSubTypes({
        @JsonSubTypes.Type(
                value = Prepare.class,
                name = "PREPARE"
        ),
        @JsonSubTypes.Type(
                value = Promise.class,
                name = "PROMISE"
        ),
        @JsonSubTypes.Type(
                value = Accept.class,
                name = "ACCEPT"
        ),
        @JsonSubTypes.Type(
                value = Accepted.class,
                name = "ACCEPTED"
        ),
        @JsonSubTypes.Type(
                value = ApplicationCommand.class,
                name = "COMMAND"
        ),
        @JsonSubTypes.Type(
                value = ApplicationResponse.class,
                name = "RESPONSE"
        ),
        @JsonSubTypes.Type(
                value = Heartbeat.class,
                name = "HEARTBEAT"
        )
})
public interface Message {

    String messageId();

    String senderId();

    String targetId();
}
