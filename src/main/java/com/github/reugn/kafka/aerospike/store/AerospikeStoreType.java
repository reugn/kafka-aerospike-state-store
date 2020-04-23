package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

public class AerospikeStoreType implements QueryableStoreType<ReadableStore> {

    // Only accept StateStores that are of type AerospikeStore
    public boolean accepts(final StateStore stateStore) {
        return stateStore instanceof AerospikeStore;
    }

    public ReadableStore create(final StateStoreProvider storeProvider, final String storeName) {
        return new AerospikeTypeWrapper(storeProvider, storeName, this);
    }

}
