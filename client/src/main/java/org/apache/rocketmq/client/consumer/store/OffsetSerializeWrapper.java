package org.apache.rocketmq.client.consumer.store;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class for offset serialization
 */
public class OffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<MessageQueue, AtomicLong>();

    public ConcurrentMap<MessageQueue, AtomicLong> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<MessageQueue, AtomicLong> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
