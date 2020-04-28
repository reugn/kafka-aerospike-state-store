package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class AerospikeStoreType<K, V> implements QueryableStoreType<ReadOnlyKeyValueStore<K, V>> {

    // Only accept StateStores that are of type AerospikeStore
    public boolean accepts(final StateStore stateStore) {
        return stateStore instanceof AerospikeStore;
    }

    public ReadOnlyKeyValueStore<K, V> create(final StateStoreProvider storeProvider, final String storeName) {
        return new AerospikeTypeWrapper<>(storeProvider, storeName, this);
    }

}
