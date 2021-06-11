package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class FilterMessageContext {
    private String consumerGroup;
    private List<MessageExt> msgList;
    private MessageQueue mq;
    private Object arg;
    private boolean unitMode;

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public List<MessageExt> getMsgList() {
        return msgList;
    }

    public void setMsgList(List<MessageExt> msgList) {
        this.msgList = msgList;
    }

    public MessageQueue getMq() {
        return mq;
    }

    public void setMq(MessageQueue mq) {
        this.mq = mq;
    }

    public Object getArg() {
        return arg;
    }

    public void setArg(Object arg) {
        this.arg = arg;
    }

    public boolean isUnitMode() {
        return unitMode;
    }

    public void setUnitMode(boolean isUnitMode) {
        this.unitMode = isUnitMode;
    }

    @Override
    public String toString() {
        return "ConsumeMessageContext [consumerGroup=" + consumerGroup + ", msgList=" + msgList + ", mq="
                + mq + ", arg=" + arg + "]";
    }
}
