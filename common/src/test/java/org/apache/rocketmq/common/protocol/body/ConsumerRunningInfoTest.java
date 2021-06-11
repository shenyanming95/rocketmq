package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.rocketmq.common.protocol.heartbeat.ConsumeType.CONSUME_ACTIVELY;
import static org.assertj.core.api.Assertions.assertThat;

public class ConsumerRunningInfoTest {

    private ConsumerRunningInfo consumerRunningInfo;

    private TreeMap<String, ConsumerRunningInfo> criTable;

    private MessageQueue messageQueue;

    @Before
    public void init() {
        consumerRunningInfo = new ConsumerRunningInfo();
        consumerRunningInfo.setJstack("test");

        TreeMap<MessageQueue, ProcessQueueInfo> mqTable = new TreeMap<MessageQueue, ProcessQueueInfo>();
        messageQueue = new MessageQueue("topicA", "broker", 1);
        mqTable.put(messageQueue, new ProcessQueueInfo());
        consumerRunningInfo.setMqTable(mqTable);

        TreeMap<String, ConsumeStatus> statusTable = new TreeMap<String, ConsumeStatus>();
        statusTable.put("topicA", new ConsumeStatus());
        consumerRunningInfo.setStatusTable(statusTable);

        TreeSet<SubscriptionData> subscriptionSet = new TreeSet<SubscriptionData>();
        subscriptionSet.add(new SubscriptionData());
        consumerRunningInfo.setSubscriptionSet(subscriptionSet);

        Properties properties = new Properties();
        properties.put(ConsumerRunningInfo.PROP_CONSUME_TYPE, CONSUME_ACTIVELY);
        properties.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, System.currentTimeMillis());
        consumerRunningInfo.setProperties(properties);

        criTable = new TreeMap<String, ConsumerRunningInfo>();
        criTable.put("client_id", consumerRunningInfo);
    }

    @Test
    public void testFromJson() {
        String toJson = RemotingSerializable.toJson(consumerRunningInfo, true);
        ConsumerRunningInfo fromJson = RemotingSerializable.fromJson(toJson, ConsumerRunningInfo.class);

        assertThat(fromJson.getJstack()).isEqualTo("test");
        assertThat(fromJson.getProperties().get(ConsumerRunningInfo.PROP_CONSUME_TYPE)).isEqualTo(ConsumeType.CONSUME_ACTIVELY.name());

        ConsumeStatus consumeStatus = fromJson.getStatusTable().get("topicA");
        assertThat(consumeStatus).isExactlyInstanceOf(ConsumeStatus.class);

        SubscriptionData subscription = fromJson.getSubscriptionSet().first();
        assertThat(subscription).isExactlyInstanceOf(SubscriptionData.class);

        ProcessQueueInfo processQueueInfo = fromJson.getMqTable().get(messageQueue);
        assertThat(processQueueInfo).isExactlyInstanceOf(ProcessQueueInfo.class);
    }

    @Test
    public void testAnalyzeRebalance() {
        boolean result = ConsumerRunningInfo.analyzeRebalance(criTable);
        assertThat(result).isTrue();
    }

    @Test
    public void testAnalyzeProcessQueue() {
        String result = ConsumerRunningInfo.analyzeProcessQueue("client_id", consumerRunningInfo);
        assertThat(result).isEmpty();

    }

    @Test
    public void testAnalyzeSubscription() {
        boolean result = ConsumerRunningInfo.analyzeSubscription(criTable);
        assertThat(result).isTrue();
    }


}
