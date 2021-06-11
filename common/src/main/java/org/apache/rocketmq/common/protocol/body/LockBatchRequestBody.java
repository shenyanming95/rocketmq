package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

public class LockBatchRequestBody extends RemotingSerializable {
    private String consumerGroup;
    private String clientId;
    private Set<MessageQueue> mqSet = new HashSet<MessageQueue>();

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Set<MessageQueue> getMqSet() {
        return mqSet;
    }

    public void setMqSet(Set<MessageQueue> mqSet) {
        this.mqSet = mqSet;
    }
}
