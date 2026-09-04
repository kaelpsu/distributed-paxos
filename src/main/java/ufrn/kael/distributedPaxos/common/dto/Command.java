package ufrn.kael.distributedPaxos.common.dto;

import java.util.Map;
import java.util.Objects;

public record Command(
        String operation,
        Map<String, String> parameters
) {

    public Command {
        if (operation == null || operation.isBlank()) {
            throw new IllegalArgumentException(
                    "Operation cannot be null or blank."
            );
        }

        Objects.requireNonNull(
                parameters,
                "Parameters cannot be null."
        );
    }
}
