package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.state.StoreBuilder;

import java.util.HashMap;
import java.util.Map;

public class AerospikeStoreBuilder<K, V> implements StoreBuilder<AerospikeStore<K, V>> {

    private Map<String, String> logConfig = new HashMap<>();
    private boolean enableLogging = false;

    private final AerospikeParamsSupplier params;
    private final String name;

    public AerospikeStoreBuilder(final AerospikeParamsSupplier params,
                                 final String name) {
        this.params = params;
        this.name = name;
    }

    @Override
    public StoreBuilder<AerospikeStore<K, V>> withCachingEnabled() {
        return this;
    }

    @Override
    public StoreBuilder<AerospikeStore<K, V>> withCachingDisabled() {
        return this;
    }

    @Override
    public StoreBuilder<AerospikeStore<K, V>> withLoggingDisabled() {
        enableLogging = false;
        return this;
    }

    @Override
    public AerospikeStore<K, V> build() {
        return new AerospikeStore<>(params, name);
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return enableLogging;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StoreBuilder<AerospikeStore<K, V>> withLoggingEnabled(final Map<String, String> config) {
        enableLogging = true;
        logConfig = config;
        return this;
    }
}
