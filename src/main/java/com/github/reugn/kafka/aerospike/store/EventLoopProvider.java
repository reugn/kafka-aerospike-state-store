package com.github.reugn.kafka.aerospike.store;

import com.aerospike.client.async.EventLoop;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NioEventLoops;

public final class EventLoopProvider {

    private EventLoopProvider() {
    }

    private static EventLoops eventLoops;

    public static EventLoop getEventLoop() {
        if (null == eventLoops) {
            eventLoops = new NioEventLoops(new EventPolicy(), 1);
        }
        return eventLoops.get(0);
    }

    public static EventLoops getEventLoops() {
        if (null == eventLoops) {
            eventLoops = new NioEventLoops(new EventPolicy(), 1);
        }
        return eventLoops;
    }
}
