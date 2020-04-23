package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.*;
import com.aerospike.client.policy.WritePolicy;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;

public class AerospikeStore implements StateStore, WriteableStore {

    private final AerospikeParamsSupplier params;
    private final String name;

    private AerospikeClient client;

    private volatile boolean open = false;
    private ProcessorContext internalProcessorContext;

    private static final String genericBinName = "g";

    public AerospikeStore(final AerospikeParamsSupplier params,
                          final String name) {
        this.params = params;
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void init(ProcessorContext context, StateStore root) {
        internalProcessorContext = context;

        if (root != null) {
            // register the store
            context.register(root, (key, value) -> put(new String(key), value));
        }

        client = new AerospikeClient(params.getPolicy(), params.getHostname(), params.getPort());
        open = true;
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() {
        if (!open) {
            return;
        }

        open = false;
        client.close();
    }

    @Override
    public boolean persistent() {
        return true;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public <V> boolean put(WritePolicy writePolicy, String key, V value) {
        validateStoreOpen();
        try {
            client.put(writePolicy,
                    new Key(params.getNamespace(), params.getSetName(), key.getBytes()),
                    new Bin(genericBinName, value));
        } catch (AerospikeException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean delete(WritePolicy writePolicy, String key) {
        validateStoreOpen();
        return client.delete(writePolicy, new Key(params.getNamespace(), params.getSetName(), key.getBytes()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> V get(String key) {
        validateStoreOpen();
        Record record = client.get(null, new Key(params.getNamespace(), params.getSetName(), key.getBytes()));
        return record == null ? null : (V) record.bins.get(genericBinName);
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

}
