package com.noctarius.hazelcast.multicast;

import com.noctarius.hazelcast.multicast.dbda.Packet;

public interface SocketContext {
    void write(Packet packet);
}
