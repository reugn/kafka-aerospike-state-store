package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PredExp;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class AerospikeStore<K, V> implements KeyValueStore<K, V> {

    private final AerospikeParamsSupplier params;
    private final String name;

    private AerospikeClient client;
    private WritePolicy writePolicy = new WritePolicy();
    private Policy readPolicy = new Policy();

    private volatile boolean open = false;
    private ProcessorContext internalProcessorContext;

    static final String genericValueBinName = "v";
    static final String genericKeyBinName = "k";

    public AerospikeStore(final AerospikeParamsSupplier params,
                          final String name) {
        this.params = params;
        this.name = name;
    }

    public AerospikeStore<K, V> setWritePolicy(WritePolicy policy) {
        this.writePolicy = policy;
        return this;
    }

    public AerospikeStore<K, V> setReadPolicy(Policy policy) {
        this.readPolicy = policy;
        return this;
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
            context.register(root, (key, value) -> put(SerializationUtils.deserialize(key),
                    SerializationUtils.deserialize(value)));
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
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    private void validateStoreOpen() {
        if (!open) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    private Key getKey(Object key) {
        return new Key(params.getNamespace(), params.getSetName(), Value.get(key));
    }

    @Override
    public void put(K key, V value) {
        Objects.requireNonNull(key, "key parameter is null");
        validateStoreOpen();
        client.put(writePolicy, getKey(key),
                new Bin(genericValueBinName, value), new Bin(genericKeyBinName, key));
    }

    @SuppressWarnings("unchecked")
    @Override
    public V putIfAbsent(K key, V value) {
        Objects.requireNonNull(key, "key parameter is null");
        validateStoreOpen();
        WritePolicy policy = new WritePolicy(writePolicy);
        policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
        Key keyObj = getKey(key);
        V rt = null;
        try {
            client.put(policy, keyObj, new Bin(genericValueBinName, value), new Bin(genericKeyBinName, key));
        } catch (AerospikeException e) {
            Record record = client.get(readPolicy, keyObj);
            if (null != record)
                rt = (V) record.bins.get(genericValueBinName);
        }
        return rt;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        Objects.requireNonNull(entries, "entries parameter is null");
        validateStoreOpen();
        for (KeyValue<K, V> kv : entries) {
            put(kv.key, kv.value);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public V delete(K key) {
        Objects.requireNonNull(key, "key parameter is null");
        validateStoreOpen();
        Key keyObj = getKey(key);
        Record record = client.get(readPolicy, keyObj);
        return client.delete(writePolicy, getKey(key)) ? (V) record.bins.get(genericValueBinName) : null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public V get(K key) {
        Objects.requireNonNull(key, "key parameter is null");
        validateStoreOpen();
        Key keyObj = getKey(key);
        Record record = client.get(readPolicy, keyObj);
        return null == record ? null : (V) record.bins.get(genericValueBinName);
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        Objects.requireNonNull(from, "from parameter is null");
        Objects.requireNonNull(to, "to parameter is null");
        validateStoreOpen();
        ScanPolicy policy = new ScanPolicy();
        policy.predExp = buildPredExp(from, to);
        return doScan(policy);
    }

    private PredExp[] buildPredExp(K from, K to) {
        List<PredExp> list = new ArrayList<>(7);
        if (from instanceof Number) {
            list.add(PredExp.integerBin(genericKeyBinName));
            list.add(PredExp.integerValue((((Number) from).longValue())));
            list.add(PredExp.integerGreaterEq());
            list.add(PredExp.integerBin(genericKeyBinName));
            list.add(PredExp.integerValue((((Number) to).longValue())));
            list.add(PredExp.integerLessEq());
            list.add(PredExp.and(2));
        } else {
            throw new UnsupportedOperationException("supported for numeric keys only");
        }
        return list.toArray(new PredExp[7]);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        validateStoreOpen();
        return doScan(new ScanPolicy());
    }

    private KeyValueIterator<K, V> doScan(ScanPolicy policy) {
        final RecordSet<K, V> recordSet = new RecordSet<>();
        RecordSequenceListener listener = new ScanRecordSequenceListener<>(recordSet);
        client.scanAll(EventLoopProvider.getEventLoop(), listener, policy,
                params.getNamespace(), params.getSetName());
        return recordSet.iterator();
    }

    @Override
    public long approximateNumEntries() {
        validateStoreOpen();
        final AtomicLong total = new AtomicLong();
        client.scanAll(null, params.getNamespace(), params.getSetName(), (k, r) -> total.incrementAndGet());
        return total.get();
    }
}
