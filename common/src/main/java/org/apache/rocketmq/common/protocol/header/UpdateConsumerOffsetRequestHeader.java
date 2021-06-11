/**
 * $Id: UpdateConsumerOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class UpdateConsumerOffsetRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String topic;
    @CFNotNull
    private Integer queueId;
    @CFNotNull
    private Long commitOffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public void setQueueId(Integer queueId) {
        this.queueId = queueId;
    }

    public Long getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(Long commitOffset) {
        this.commitOffset = commitOffset;
    }
}
