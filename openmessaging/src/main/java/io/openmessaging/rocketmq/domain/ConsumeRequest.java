package io.openmessaging.rocketmq.domain;

import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

public class ConsumeRequest {
    private final MessageExt messageExt;
    private final MessageQueue messageQueue;
    private final ProcessQueue processQueue;
    private long startConsumeTimeMillis;

    public ConsumeRequest(final MessageExt messageExt, final MessageQueue messageQueue,
                          final ProcessQueue processQueue) {
        this.messageExt = messageExt;
        this.messageQueue = messageQueue;
        this.processQueue = processQueue;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }

    public MessageQueue getMessageQueue() {
        return messageQueue;
    }

    public ProcessQueue getProcessQueue() {
        return processQueue;
    }

    public long getStartConsumeTimeMillis() {
        return startConsumeTimeMillis;
    }

    public void setStartConsumeTimeMillis(final long startConsumeTimeMillis) {
        this.startConsumeTimeMillis = startConsumeTimeMillis;
    }
}
