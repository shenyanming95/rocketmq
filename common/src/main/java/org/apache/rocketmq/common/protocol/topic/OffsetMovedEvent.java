package org.apache.rocketmq.common.protocol.topic;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class OffsetMovedEvent extends RemotingSerializable {
    private String consumerGroup;
    private MessageQueue messageQueue;
    private long offsetRequest;
    private long offsetNew;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getOffsetRequest() {
        return offsetRequest;
    }

    public void setOffsetRequest(long offsetRequest) {
        this.offsetRequest = offsetRequest;
    }

    public long getOffsetNew() {
        return offsetNew;
    }

    public void setOffsetNew(long offsetNew) {
        this.offsetNew = offsetNew;
    }

    @Override
    public String toString() {
        return "OffsetMovedEvent [consumerGroup=" + consumerGroup + ", messageQueue=" + messageQueue + ", offsetRequest=" + offsetRequest + ", offsetNew=" + offsetNew + "]";
    }
}
