package org.apache.rocketmq.common.admin;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

public class TopicStatsTable extends RemotingSerializable {
    private HashMap<MessageQueue, TopicOffset> offsetTable = new HashMap<MessageQueue, TopicOffset>();

    public HashMap<MessageQueue, TopicOffset> getOffsetTable() {
        return offsetTable;
    }

    public void setOffsetTable(HashMap<MessageQueue, TopicOffset> offsetTable) {
        this.offsetTable = offsetTable;
    }
}
