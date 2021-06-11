package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ConsumeStatsListTest {

    @Test
    public void testFromJson() {
        ConsumeStats consumeStats = new ConsumeStats();
        ArrayList<ConsumeStats> consumeStatsListValue = new ArrayList<ConsumeStats>();
        consumeStatsListValue.add(consumeStats);
        HashMap<String, List<ConsumeStats>> map = new HashMap<String, List<ConsumeStats>>();
        map.put("subscriptionGroupName", consumeStatsListValue);
        List<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsListValue2 = new ArrayList<Map<String, List<ConsumeStats>>>();
        consumeStatsListValue2.add(map);

        String brokerAddr = "brokerAddr";
        long totalDiff = 12352L;
        ConsumeStatsList consumeStatsList = new ConsumeStatsList();
        consumeStatsList.setBrokerAddr(brokerAddr);
        consumeStatsList.setTotalDiff(totalDiff);
        consumeStatsList.setConsumeStatsList(consumeStatsListValue2);

        String toJson = RemotingSerializable.toJson(consumeStatsList, true);
        ConsumeStatsList fromJson = RemotingSerializable.fromJson(toJson, ConsumeStatsList.class);

        assertThat(fromJson.getBrokerAddr()).isEqualTo(brokerAddr);
        assertThat(fromJson.getTotalDiff()).isEqualTo(totalDiff);

        List<Map<String, List<ConsumeStats>>> fromJsonConsumeStatsList = fromJson.getConsumeStatsList();
        assertThat(fromJsonConsumeStatsList).isInstanceOf(List.class);

        ConsumeStats fromJsonConsumeStats = fromJsonConsumeStatsList.get(0).get("subscriptionGroupName").get(0);
        assertThat(fromJsonConsumeStats).isExactlyInstanceOf(ConsumeStats.class);
    }
}