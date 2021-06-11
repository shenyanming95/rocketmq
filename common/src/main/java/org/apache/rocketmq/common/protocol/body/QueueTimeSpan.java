package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Date;

public class QueueTimeSpan {
    private MessageQueue messageQueue;
    private long minTimeStamp;
    private long maxTimeStamp;
    private long consumeTimeStamp;
    private long delayTime;

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

    public long getMinTimeStamp() {
        return minTimeStamp;
    }

    public void setMinTimeStamp(long minTimeStamp) {
        this.minTimeStamp = minTimeStamp;
    }

    public long getMaxTimeStamp() {
        return maxTimeStamp;
    }

    public void setMaxTimeStamp(long maxTimeStamp) {
        this.maxTimeStamp = maxTimeStamp;
    }

    public long getConsumeTimeStamp() {
        return consumeTimeStamp;
    }

    public void setConsumeTimeStamp(long consumeTimeStamp) {
        this.consumeTimeStamp = consumeTimeStamp;
    }

    public String getMinTimeStampStr() {
        return UtilAll.formatDate(new Date(minTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }

    public String getMaxTimeStampStr() {
        return UtilAll.formatDate(new Date(maxTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }

    public String getConsumeTimeStampStr() {
        return UtilAll.formatDate(new Date(consumeTimeStamp), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
    }

    public long getDelayTime() {
        return delayTime;
    }

    public void setDelayTime(long delayTime) {
        this.delayTime = delayTime;
    }
}
