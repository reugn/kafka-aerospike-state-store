package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AerospikeStoreIterator<K, V> implements KeyValueIterator<K, V> {

    private enum State {
        OK, DONE, FAILED
    }

    private static final int defaultCapacity = 1024;

    private BlockingQueue<KeyValue<K, V>> queue = new LinkedBlockingQueue<>(defaultCapacity);
    private BlockingQueue<Boolean> monitor = new LinkedBlockingQueue<>(defaultCapacity);

    private State state = State.OK;

    @Override
    public void close() {
        if (state == State.OK)
            state = State.DONE;
        updateMonitor(false);
    }

    public void fail() {
        if (state == State.OK)
            state = State.FAILED;
        updateMonitor(false);
    }

    @Override
    public K peekNextKey() {
        KeyValue<K, V> kv = queue.peek();
        return null == kv ? null : kv.key;
    }

    @Override
    public boolean hasNext() {
        return state == State.OK && validateMonitor();
    }

    @Override
    public KeyValue<K, V> next() {
        try {
            return queue.take();
        } catch (InterruptedException e) {
            throw new NoSuchElementException();
        }
    }

    public void put(KeyValue<K, V> kv) {
        boolean v = false;
        try {
            if (state == State.OK) {
                queue.put(kv);
                v = true;
            }
        } catch (InterruptedException e) {
            state = State.FAILED;
        }
        updateMonitor(v);
    }

    private void updateMonitor(boolean v) {
        try {
            monitor.put(v);
        } catch (InterruptedException e) {
            state = State.FAILED;
        }
    }

    private boolean validateMonitor() {
        try {
            return monitor.take();
        } catch (InterruptedException e) {
            state = State.FAILED;
            return false;
        }
    }
}
