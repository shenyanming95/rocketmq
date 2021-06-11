package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

public class ProducerConnection extends RemotingSerializable {
    private HashSet<Connection> connectionSet = new HashSet<Connection>();

    public HashSet<Connection> getConnectionSet() {
        return connectionSet;
    }

    public void setConnectionSet(HashSet<Connection> connectionSet) {
        this.connectionSet = connectionSet;
    }
}
