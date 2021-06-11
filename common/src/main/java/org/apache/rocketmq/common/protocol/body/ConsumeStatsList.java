package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConsumeStatsList extends RemotingSerializable {
    private List<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>> consumeStatsList = new ArrayList<Map<String/*subscriptionGroupName*/, List<ConsumeStats>>>();
    private String brokerAddr;
    private long totalDiff;

    public List<Map<String, List<ConsumeStats>>> getConsumeStatsList() {
        return consumeStatsList;
    }

    public void setConsumeStatsList(List<Map<String, List<ConsumeStats>>> consumeStatsList) {
        this.consumeStatsList = consumeStatsList;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public long getTotalDiff() {
        return totalDiff;
    }

    public void setTotalDiff(long totalDiff) {
        this.totalDiff = totalDiff;
    }
}
