# kafka-aerospike-state-store
[![Build](https://github.com/reugn/kafka-aerospike-state-store/actions/workflows/build.yml/badge.svg)](https://github.com/reugn/kafka-aerospike-state-store/actions/workflows/build.yml)

[Apache Kafka](https://kafka.apache.org/) StateStore is a storage engine for managing state maintained by a stream processor.

This repo implements custom persistent StateStore backed by [Aerospike](https://www.aerospike.com/) Database.

Only the [Processor API](https://kafka.apache.org/20/documentation/streams/developer-guide/processor-api.html#streams-developer-guide-processor-api) supports custom state stores.

## Getting started
### Build from source
```sh
./gradlew clean build
```

### Install as a dependency
Read on [how to install](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#installing-a-package) the `kafka-aerospike-state-store` package from GitHub Packages.

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