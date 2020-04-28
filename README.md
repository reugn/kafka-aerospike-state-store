# kafka-aerospike-state-store
[ ![Download](https://api.bintray.com/packages/reug/maven/kafka-aerospike-state-store/images/download.svg) ](https://bintray.com/reug/maven/kafka-aerospike-state-store/_latestVersion)
[![Build Status](https://travis-ci.com/reugn/kafka-aerospike-state-store.svg?branch=master)](https://travis-ci.com/reugn/kafka-aerospike-state-store)

Kafka StateStore is a storage engine for managing state maintained by a stream processor.

This repo implements custom persistent StateStore backed by [Aerospike](https://www.aerospike.com/) database.

Only the [Processor API](https://kafka.apache.org/20/documentation/streams/developer-guide/processor-api.html#streams-developer-guide-processor-api) supports custom state stores.

## Setup
Add Maven/Gradle repository
```
https://dl.bintray.com/reug/maven
```
Gradle dependency
```
compile(group: 'com.github.reugn', name: 'kafka-aerospike-state-store', version: '<version>')
```

## Example
```java
Topology topology = new Topology();
topology.addSource("source", INPUT_TOPIC);
topology.addProcessor("processor", new StoreProcessorSupplier(), "source");
topology.addStateStore(new AerospikeStoreBuilder(
        new AerospikeParamsSupplier("localhost", 3000, "test", "store"),
        storeName), "processor");
topology.addSink("sink", OUTPUT_TOPIC, "processor");
```

## License
Licensed under the Apache 2.0 License.