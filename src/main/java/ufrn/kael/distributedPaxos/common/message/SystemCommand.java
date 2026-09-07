package ufrn.kael.distributedPaxos.common.message;

import java.util.Map;

public record SystemCommand(
        String operation,
        Map<String, String> parameters
) {

    public SystemCommand {
        if (operation == null || operation.isBlank()) {
            throw new IllegalArgumentException(
                    "Operation cannot be null or blank."
            );
        }

        if (parameters == null) {
            throw new IllegalArgumentException(
                    "Parameters cannot be null."
            );
        }
    }
}