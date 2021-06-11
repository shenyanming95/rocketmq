package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;

public class PushConsumerWithNamespace {
    public static void main(String[] args) throws Exception {
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("InstanceTest", "cidTest");
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        defaultMQPushConsumer.subscribe("topicTest", "*");
        defaultMQPushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            msgs.stream().forEach((msg) -> {
                System.out.printf("Msg topic is:%s, MsgId is:%s, reconsumeTimes is:%s%n", msg.getTopic(), msg.getMsgId(), msg.getReconsumeTimes());
            });
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

        defaultMQPushConsumer.start();
    }
}