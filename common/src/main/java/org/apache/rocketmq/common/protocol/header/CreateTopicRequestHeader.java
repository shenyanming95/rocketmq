/**
 * $Id: CreateTopicRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CreateTopicRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String defaultTopic;
    @CFNotNull
    private Integer readQueueNums;
    @CFNotNull
    private Integer writeQueueNums;
    @CFNotNull
    private Integer perm;
    @CFNotNull
    private String topicFilterType;
    private Integer topicSysFlag;
    @CFNotNull
    private Boolean order = false;

    @Override
    public void checkFields() throws RemotingCommandException {
        try {
            TopicFilterType.valueOf(this.topicFilterType);
        } catch (Exception e) {
            throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
        }
    }

    public TopicFilterType getTopicFilterTypeEnum() {
        return TopicFilterType.valueOf(this.topicFilterType);
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }

    public Integer getReadQueueNums() {
        return readQueueNums;
    }

    public void setReadQueueNums(Integer readQueueNums) {
        this.readQueueNums = readQueueNums;
    }

    public Integer getWriteQueueNums() {
        return writeQueueNums;
    }

    public void setWriteQueueNums(Integer writeQueueNums) {
        this.writeQueueNums = writeQueueNums;
    }

    public Integer getPerm() {
        return perm;
    }

    public void setPerm(Integer perm) {
        this.perm = perm;
    }

    public String getTopicFilterType() {
        return topicFilterType;
    }

    public void setTopicFilterType(String topicFilterType) {
        this.topicFilterType = topicFilterType;
    }

    public Integer getTopicSysFlag() {
        return topicSysFlag;
    }

    public void setTopicSysFlag(Integer topicSysFlag) {
        this.topicSysFlag = topicSysFlag;
    }

    public Boolean getOrder() {
        return order;
    }

    public void setOrder(Boolean order) {
        this.order = order;
    }
}
