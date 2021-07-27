package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class GetConsumerStatusBody extends RemotingSerializable {
    private Map<MessageQueue, Long> messageQueueTable = new HashMap<MessageQueue, Long>();
    private Map<String, Map<MessageQueue, Long>> consumerTable = new HashMap<String, Map<MessageQueue, Long>>();

    public Map<MessageQueue, Long> getMessageQueueTable() {
        return messageQueueTable;
    }

    public void setMessageQueueTable(Map<MessageQueue, Long> messageQueueTable) {
        this.messageQueueTable = messageQueueTable;
    }

    public Map<String, Map<MessageQueue, Long>> getConsumerTable() {
        return consumerTable;
    }

    public void setConsumerTable(Map<String, Map<MessageQueue, Long>> consumerTable) {
        this.consumerTable = consumerTable;
    }
}
