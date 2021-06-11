package org.apache.rocketmq.example.namespace;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

public class ProducerWithNamespace {
    public static void main(String[] args) throws Exception {

        DefaultMQProducer producer = new DefaultMQProducer("InstanceTest", "pidTest");
        producer.setNamesrvAddr("127.0.0.1:9876");
        producer.start();
        for (int i = 0; i < 100; i++) {
            Message message = new Message("topicTest", "tagTest", "Hello world".getBytes());
            try {
                SendResult result = producer.send(message);
                System.out.printf("Topic:%s send success, misId is:%s%n", message.getTopic(), result.getMsgId());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}