package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

public interface KeyValueIterable<K, V> {

    /**
     * Returns an Iterator interface of {@link KeyValue}.
     *
     * @return an Iterator interface of {@link KeyValue}.
     */
    KeyValueIterator<K, V> iterator();
}
