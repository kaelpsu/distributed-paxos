package ufrn.kael.distributedPaxos.stateful.state;

import java.util.HashMap;
import java.util.Map;

public class InMemoryDatabase {

    private final Map<String, String> data = new HashMap<>();

    public void set(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }

    public void delete(String key) {
        data.remove(key);
    }
}