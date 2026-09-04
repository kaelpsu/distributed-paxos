package ufrn.kael.distributedPaxos.common.message;

public record NodeId(String value) {

    public NodeId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("NodeId cannot be null or blank.");
        }
    }

    @Override
    public String toString() {
        return value;
    }
    
}
