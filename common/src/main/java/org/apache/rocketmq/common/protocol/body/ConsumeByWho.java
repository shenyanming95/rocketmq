package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

public class ConsumeByWho extends RemotingSerializable {
    private HashSet<String> consumedGroup = new HashSet<String>();
    private HashSet<String> notConsumedGroup = new HashSet<String>();
    private String topic;
    private int queueId;
    private long offset;

    public HashSet<String> getConsumedGroup() {
        return consumedGroup;
    }

    public void setConsumedGroup(HashSet<String> consumedGroup) {
        this.consumedGroup = consumedGroup;
    }

    public HashSet<String> getNotConsumedGroup() {
        return notConsumedGroup;
    }

    public void setNotConsumedGroup(HashSet<String> notConsumedGroup) {
        this.notConsumedGroup = notConsumedGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
