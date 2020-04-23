package com.github.reugn.kafka.aerospike.store;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Properties;

public class AerospikeStoreTest {

    private TopologyTestDriver testDriver;
    private AerospikeStore store;
    static final String storeName = "test-store";

    private final String INPUT_TOPIC = "in";
    private final String OUTPUT_TOPIC = "out";

    private StringDeserializer stringDeserializer = new StringDeserializer();
    private LongDeserializer longDeserializer = new LongDeserializer();

    private StringSerializer stringSerializer = new StringSerializer();
    private LongSerializer longSerializer = new LongSerializer();

    private TestInputTopic<String, Long> recordFactory;
    private TestOutputTopic<String, Long> recordSink;

    @BeforeEach
    public void setup() {
        Topology topology = new Topology();
        topology.addSource("source", INPUT_TOPIC);
        topology.addProcessor("processor", new StoreProcessorSupplier(), "source");
        topology.addStateStore(new AerospikeStoreBuilder(
                new AerospikeParamsSupplier("localhost", 3000, "test", "store"),
                storeName), "processor");
        topology.addSink("sink", OUTPUT_TOPIC, "processor");

        // setup test driver
        Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "storeTest");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "host:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
        testDriver = new TopologyTestDriver(topology, props);

        recordFactory = testDriver.createInputTopic(INPUT_TOPIC, stringSerializer, longSerializer);
        recordSink = testDriver.createOutputTopic(OUTPUT_TOPIC, stringDeserializer, longDeserializer);

        // pre-populate store
        store = (AerospikeStore) testDriver.getStateStore(storeName);
        store.put("key", 21L);
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldNotUpdateStoreForSmallerValue() {
        recordFactory.pipeInput("key", 10L, 9999L);
        Assertions.assertEquals(store.<Long>get("key").longValue(), 21L);
        Assertions.assertEquals(recordSink.readKeyValue(), new KeyValue<>("key", 10L));
        Assertions.assertThrows(NoSuchElementException.class, () -> recordSink.readKeyValue());
    }

    @Test
    public void shouldUpdateStoreForLargerValue() {
        recordFactory.pipeInput("key", 100L, 9999L);
        Assertions.assertEquals(store.<Long>get("key").longValue(), 100L);
        Assertions.assertEquals(recordSink.readKeyValue(), new KeyValue<>("key", 100L));
        Assertions.assertThrows(NoSuchElementException.class, () -> recordSink.readKeyValue());
    }

    @Test
    public void shouldDeleteValue() {
        recordFactory.pipeInput("key", 0L, 9999L);
        Assertions.assertNull(store.<Long>get("key"));
        Assertions.assertEquals(recordSink.readKeyValue(), new KeyValue<>("key", 0L));
        Assertions.assertThrows(NoSuchElementException.class, () -> recordSink.readKeyValue());
    }

    static class StoreProcessorSupplier implements ProcessorSupplier<String, Long> {
        @Override
        public Processor<String, Long> get() {
            return new StoreProcessor();
        }
    }

    static class StoreProcessor implements Processor<String, Long> {
        private ProcessorContext context;
        private AerospikeStore store;

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            store = (AerospikeStore) context.getStateStore(storeName);
        }

        @Override
        public void process(String key, Long value) {
            Long oldValue = store.get(key);
            if (oldValue == null || value > oldValue) {
                store.put(key, value);
            }
            if(value == 0) {
                store.delete(key);
            }
            context.forward(key, value);
        }

        @Override
        public void close() {
        }
    }

}
