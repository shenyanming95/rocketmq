package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

public interface TopicMessageQueueChangeListener {
    /**
     * This method will be invoked in the condition of queue numbers changed, These scenarios occur when the topic is
     * expanded or shrunk.
     *
     * @param messageQueues
     */
    void onChanged(String topic, Set<MessageQueue> messageQueues);
}
