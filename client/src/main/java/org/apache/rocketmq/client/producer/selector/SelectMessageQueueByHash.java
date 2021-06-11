package org.apache.rocketmq.client.producer.selector;

import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class SelectMessageQueueByHash implements MessageQueueSelector {

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        int value = arg.hashCode() % mqs.size();
        if (value < 0) {
            value = Math.abs(value);
        }
        return mqs.get(value);
    }
}
