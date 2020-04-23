package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.policy.WritePolicy;

public interface WriteableStore extends ReadableStore {

    public <V> boolean put(WritePolicy writePolicy, String key, V value);

    public default <V> boolean put(String key, V value) {
        return put(null, key, value);
    }

    public boolean delete(WritePolicy writePolicy, String key);

    public default boolean delete(String key) {
        return delete(null, key);
    }
}
