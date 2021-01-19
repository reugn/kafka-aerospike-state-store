package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class RecordSet<K, V> implements KeyValueIterable<K, V>, Closeable {

    private static final int defaultCapacity = 1024;

    public final KeyValue<K, V> END = new KeyValue<>(null, null);

    private final BlockingQueue<KeyValue<K, V>> queue = new LinkedBlockingQueue<>(defaultCapacity);
    private KeyValue<K, V> record;
    private boolean valid = true;

    /**
     * Retrieve next record. This method will block until a record is retrieved
     * or the query is cancelled.
     *
     * @return whether record exists - if false, no more records are available
     */
    public boolean next() {
        if (!valid) {
            return false;
        }

        try {
            record = queue.take();
        } catch (InterruptedException ie) {
            valid = false;
            return false;
        }

        if (record == END) {
            valid = false;
            return false;
        }

        return true;
    }

    @Override
    public void close() {
        valid = false;
    }

    /**
     * Put a record on the queue.
     */
    public boolean put(KeyValue<K, V> record) {
        if (!valid) {
            return false;
        }

        try {
            // This put will block if queue capacity is reached.
            queue.put(record);
            return true;
        } catch (InterruptedException ie) {
            // Valid may have changed. Check again.
            if (valid) {
                abort();
            }
            return false;
        }
    }

    /**
     * Abort retrieval with the end token.
     */
    protected void abort() {
        valid = false;
        queue.clear();
        while (true)
            if (queue.offer(END) && queue.poll() == null) return;
    }

    /**
     * Returns an Iterator interface of {@link KeyValue}.
     */
    @Override
    public KeyValueIterator<K, V> iterator() {
        return new RecordSetIterator(this);
    }

    /**
     * Support standard iteration interface for {@link KeyValue}.
     */
    private class RecordSetIterator implements KeyValueIterator<K, V>, Closeable {

        private final RecordSet<K, V> recordSet;
        private boolean more;

        RecordSetIterator(RecordSet<K, V> recordSet) {
            this.recordSet = recordSet;
            more = this.recordSet.next();
        }

        @Override
        public boolean hasNext() {
            return more;
        }

        @Override
        public KeyValue<K, V> next() {
            KeyValue<K, V> kv = recordSet.record;
            more = recordSet.next();
            return kv;
        }

        @Override
        public void remove() {
        }

        @Override
        public void close() {
            recordSet.close();
        }

        @Override
        public K peekNextKey() {
            KeyValue<K, V> kv = recordSet.record;
            return kv.key;
        }
    }
}
