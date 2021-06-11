package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

public class ResetOffsetBodyForC extends RemotingSerializable {

    private List<MessageQueueForC> offsetTable;

    public List<MessageQueueForC> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(List<MessageQueueForC> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
