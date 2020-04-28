package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.policy.ClientPolicy;

public class AerospikeParamsSupplier {

    private final ClientPolicy policy;
    private final String namespace;
    private final String setName;
    private final String hostname;
    private final int port;

    public AerospikeParamsSupplier(final ClientPolicy policy,
                                   final String hostname,
                                   final int port,
                                   final String namespace,
                                   final String setName) {
        if (policy.eventLoops == null)
            policy.eventLoops = EventLoopProvider.getEventLoops();
        this.policy = policy;
        this.hostname = hostname;
        this.port = port;
        this.namespace = namespace;
        this.setName = setName;
    }

    public AerospikeParamsSupplier(final String hostname,
                                   final int port,
                                   final String namespace,
                                   final String setName) {
        this(new ClientPolicy(), hostname, port, namespace, setName);
    }

    public AerospikeParamsSupplier(final String hostname,
                                   final int port,
                                   final String namespace) {
        this(new ClientPolicy(), hostname, port, namespace, null);
    }

    public AerospikeParamsSupplier(final String hostname,
                                   final String namespace) {
        this(new ClientPolicy(), hostname, 3000, namespace, null);
    }

    public ClientPolicy getPolicy() {
        return policy;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getNamespace() {
        return namespace;
    }

    public String getSetName() {
        return setName;
    }

}
