package org.apache.rocketmq.store.schedule;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DelayOffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable = new ConcurrentHashMap<Integer, Long>(32);

    public ConcurrentMap<Integer, Long> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(ConcurrentMap<Integer, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
