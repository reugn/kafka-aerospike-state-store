package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

public class AerospikeStoreTest {

    private TopologyTestDriver testDriver;
    private KeyValueStore<Long, Long> store;
    static final String storeName = "test-store";

    private final String INPUT_TOPIC = "in";
    private final String OUTPUT_TOPIC = "out";

    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();
    private IntegerDeserializer intDeserializer = new IntegerDeserializer();

    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();
    private IntegerSerializer intSerializer = new IntegerSerializer();

    private TestInputTopic<Long, Long> recordFactory;
    private TestOutputTopic<Long, Long> recordSink;

    private List<KeyValue<Long, Long>> records;
    private List<Long> keysToDelete;

    @BeforeEach
    public void setup() {
        Topology topology = new Topology();
        topology.addSource("source", INPUT_TOPIC);
        topology.addProcessor("processor", new StoreProcessorSupplier(), "source");
        topology.addStateStore(new AerospikeStoreBuilder<Integer, Integer>(
                new AerospikeParamsSupplier("localhost", 3000, "test", "store"),
                storeName), "processor");
        topology.addSink("sink", OUTPUT_TOPIC, "processor");

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "storeTest");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        recordFactory = testDriver.createInputTopic(INPUT_TOPIC, longSerializer, longSerializer);
        recordSink = testDriver.createOutputTopic(OUTPUT_TOPIC, longDeserializer, longDeserializer);

        // build records List
        records = new ArrayList<>();
        records.add(new KeyValue<>(1L, 112L));
        records.add(new KeyValue<>(14L, 135L));
        records.add(new KeyValue<>(25L, 119L));
        records.add(new KeyValue<>(37L, 144L));
        records.add(new KeyValue<>(49L, 150L));

        keysToDelete = new ArrayList<>();

        // pre-populate store
        store = testDriver.getKeyValueStore(storeName);
        store.putAll(records);
    }

    @AfterEach
    public void tearDown() {
        for (Long key : keysToDelete) {
            store.delete(key);
        }
        testDriver.close();
    }

    @Test
    public void shouldPutValue() {
        recordFactory.pipeInput(2L, 105L, 9999L);
        Assertions.assertEquals(store.get(2L).longValue(), 105L);
        Assertions.assertEquals(recordSink.readKeyValue(), new KeyValue<>(2L, 105L));
        Assertions.assertThrows(NoSuchElementException.class, () -> recordSink.readKeyValue());
        keysToDelete.add(2L);
    }

    @Test
    public void shouldDeleteValue() {
        recordFactory.pipeInput(1L, 0L, 9999L);
        Assertions.assertNull(store.get(1L));
        Assertions.assertEquals(recordSink.readKeyValue(), new KeyValue<>(1L, 0L));
        Assertions.assertThrows(NoSuchElementException.class, () -> recordSink.readKeyValue());
    }

    @Test
    public void shouldPutValueIfAbsent() {
        Long exists = store.putIfAbsent(1L, 111L);
        Assertions.assertEquals(exists, 112L);
        Long absent = store.putIfAbsent(2L, 111L);
        Assertions.assertNull(absent);
        Assertions.assertEquals(store.get(2L).longValue(), 111L);
        keysToDelete.add(2L);
    }

    @Test
    public void shouldReturnApproximateNumEntries() {
        Long num = store.approximateNumEntries();
        Assertions.assertEquals(num, 5);
    }

    @Test
    public void shouldReturnAll() {
        KeyValueIterator<Long, Long> iter = store.all();
        List<KeyValue<Long, Long>> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        actual.sort(Comparator.comparing(kv -> kv.key));
        Assertions.assertEquals(actual, records);
    }

    @Test
    public void shouldReturnRange() {
        KeyValueIterator<Long, Long> iter = store.range(10L, 30L);
        List<KeyValue<Long, Long>> actual = new ArrayList<>();
        while (iter.hasNext()) {
            actual.add(iter.next());
        }
        actual.sort(Comparator.comparing(kv -> kv.key));
        List<KeyValue<Long, Long>> expected = new ArrayList<>();
        expected.add(records.get(1));
        expected.add(records.get(2));
        Assertions.assertEquals(actual, expected);
    }

    static class StoreProcessorSupplier implements ProcessorSupplier<Long, Long> {
        @Override
        public Processor<Long, Long> get() {
            return new StoreProcessor();
        }
    }

    static class StoreProcessor implements Processor<Long, Long> {
        private ProcessorContext context;
        private KeyValueStore<Long, Long> store;

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            store = (KeyValueStore<Long, Long>) context.getStateStore(storeName);
        }

        @Override
        public void process(Long key, Long value) {
            store.put(key, value);
            if (value == 0) {
                store.delete(key);
            }
            context.forward(key, value);
        }

        @Override
        public void close() {
        }
    }

}
