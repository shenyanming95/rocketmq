package org.apache.rocketmq.common.protocol.route;


import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


public class TopicRouteDataTest {
    @Test
    public void testTopicRouteDataClone() throws Exception {

        TopicRouteData topicRouteData = new TopicRouteData();

        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setPerm(6);
        queueData.setReadQueueNums(8);
        queueData.setWriteQueueNums(8);
        queueData.setTopicSynFlag(0);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        queueDataList.add(queueData);

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "192.168.0.47:10911");
        brokerAddrs.put(1L, "192.168.0.47:10921");

        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerData.setBrokerName("broker-a");
        brokerData.setCluster("TestCluster");

        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        brokerDataList.add(brokerData);

        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setQueueDatas(queueDataList);

        assertThat(topicRouteData.cloneTopicRouteData()).isEqualTo(topicRouteData);

    }

    @Test
    public void testTopicRouteDataJsonSerialize() throws Exception {

        TopicRouteData topicRouteData = new TopicRouteData();

        QueueData queueData = new QueueData();
        queueData.setBrokerName("broker-a");
        queueData.setPerm(6);
        queueData.setReadQueueNums(8);
        queueData.setWriteQueueNums(8);
        queueData.setTopicSynFlag(0);

        List<QueueData> queueDataList = new ArrayList<QueueData>();
        queueDataList.add(queueData);

        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(0L, "192.168.0.47:10911");
        brokerAddrs.put(1L, "192.168.0.47:10921");

        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerAddrs(brokerAddrs);
        brokerData.setBrokerName("broker-a");
        brokerData.setCluster("TestCluster");

        List<BrokerData> brokerDataList = new ArrayList<BrokerData>();
        brokerDataList.add(brokerData);

        topicRouteData.setBrokerDatas(brokerDataList);
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setQueueDatas(queueDataList);

        String topicRouteDataJsonStr = RemotingSerializable.toJson(topicRouteData, true);
        TopicRouteData topicRouteDataFromJson = RemotingSerializable.fromJson(topicRouteDataJsonStr, TopicRouteData.class);

        assertThat(topicRouteDataJsonStr).isNotEqualTo(topicRouteDataFromJson);
        assertThat(topicRouteDataFromJson.getBrokerDatas()).isEqualTo(topicRouteData.getBrokerDatas());
        assertThat(topicRouteDataFromJson.getFilterServerTable()).isEqualTo(topicRouteData.getFilterServerTable());
        assertThat(topicRouteDataFromJson.getQueueDatas()).isEqualTo(topicRouteData.getQueueDatas());

    }
}
