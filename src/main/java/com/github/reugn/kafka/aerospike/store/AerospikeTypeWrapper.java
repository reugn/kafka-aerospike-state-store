package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Optional;

public class AerospikeTypeWrapper implements ReadableStore {

    private final QueryableStoreType<ReadableStore> storeType;
    private final String storeName;
    private final StateStoreProvider provider;

    public AerospikeTypeWrapper(final StateStoreProvider provider,
                                final String storeName,
                                final QueryableStoreType<ReadableStore> storeType) {

        this.provider = provider;
        this.storeName = storeName;
        this.storeType = storeType;
    }

    @Override
    public <V> V get(String key) {
        final List<ReadableStore> stores = provider.stores(storeName, storeType);
        final Optional<V> value = stores.stream().filter(s -> s.get(key) != null).findFirst().map(s -> s.get(key));
        return value.orElse(null);
    }

}
