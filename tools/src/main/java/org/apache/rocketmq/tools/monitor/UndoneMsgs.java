package org.apache.rocketmq.tools.monitor;

public class UndoneMsgs {
    private String consumerGroup;
    private String topic;

    private long undoneMsgsTotal;

    private long undoneMsgsSingleMQ;

    private long undoneMsgsDelayTimeMills;

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

    public long getUndoneMsgsTotal() {
        return undoneMsgsTotal;
    }

    public void setUndoneMsgsTotal(long undoneMsgsTotal) {
        this.undoneMsgsTotal = undoneMsgsTotal;
    }

    public long getUndoneMsgsSingleMQ() {
        return undoneMsgsSingleMQ;
    }

    public void setUndoneMsgsSingleMQ(long undoneMsgsSingleMQ) {
        this.undoneMsgsSingleMQ = undoneMsgsSingleMQ;
    }

    public long getUndoneMsgsDelayTimeMills() {
        return undoneMsgsDelayTimeMills;
    }

    public void setUndoneMsgsDelayTimeMills(long undoneMsgsDelayTimeMills) {
        this.undoneMsgsDelayTimeMills = undoneMsgsDelayTimeMills;
    }

    @Override
    public String toString() {
        return "UndoneMsgs [consumerGroup=" + consumerGroup + ", topic=" + topic + ", undoneMsgsTotal="
                + undoneMsgsTotal + ", undoneMsgsSingleMQ=" + undoneMsgsSingleMQ
                + ", undoneMsgsDelayTimeMills=" + undoneMsgsDelayTimeMills + "]";
    }
}
