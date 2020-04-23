package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.HashMap;
import java.util.Map;

public class AerospikeStoreBuilder implements StoreBuilder {

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
    public StoreBuilder withCachingEnabled() {
        return this;
    }

    @Override
    public StoreBuilder withCachingDisabled() {
        return this;
    }

    @Override
    public StoreBuilder withLoggingDisabled() {
        enableLogging = false;
        return this;
    }

    @Override
    public StateStore build() {
        return new AerospikeStore(params, name);
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
    public StoreBuilder withLoggingEnabled(Map config) {
        enableLogging = true;
        logConfig = config;
        return this;
    }
}
