package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.listener.RecordSequenceListener;
import org.apache.kafka.streams.KeyValue;

public class ScanRecordSequenceListener<K, V> implements RecordSequenceListener {

    private final RecordSet<K, V> recordSet;

    ScanRecordSequenceListener(RecordSet<K, V> recordSet) {
        this.recordSet = recordSet;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onRecord(Key key, Record record) throws AerospikeException {
        K k = (K) record.bins.get(AerospikeStore.genericKeyBinName);
        V v = (V) record.bins.get(AerospikeStore.genericValueBinName);
        recordSet.put(new KeyValue<>(k, v));
    }

    @Override
    public void onSuccess() {
        recordSet.put(recordSet.END);
    }

    @Override
    public void onFailure(AerospikeException exception) {
        recordSet.close();
    }
}
