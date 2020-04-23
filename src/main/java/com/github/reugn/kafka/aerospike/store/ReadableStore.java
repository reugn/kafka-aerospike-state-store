package com.github.reugn.kafka.aerospike.store;

public interface ReadableStore {

    public <V> V get(String key);
}
