package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.Map;

public class ResetOffsetBody extends RemotingSerializable {
    private Map<MessageQueue, Long> offsetTable;

    public Map<MessageQueue, Long> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(Map<MessageQueue, Long> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
