package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Optional;

public class AerospikeTypeWrapper<K, V> implements ReadOnlyKeyValueStore<K, V> {

    private final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> storeType;
    private final String storeName;
    private final StateStoreProvider provider;

    public AerospikeTypeWrapper(final StateStoreProvider provider,
                                final String storeName,
                                final QueryableStoreType<ReadOnlyKeyValueStore<K, V>> storeType) {

        this.provider = provider;
        this.storeName = storeName;
        this.storeType = storeType;
    }

    @Override
    public V get(K key) {
        final List<ReadOnlyKeyValueStore<K, V>> stores = provider.stores(storeName, storeType);
        final Optional<V> value = stores.stream().filter(s -> s.get(key) != null).findFirst().map(s -> s.get(key));
        return value.orElse(null);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        final List<ReadOnlyKeyValueStore<K, V>> stores = provider.stores(storeName, storeType);
        return stores.stream().map(ReadOnlyKeyValueStore::approximateNumEntries).reduce(0L, Long::sum);
    }
}
