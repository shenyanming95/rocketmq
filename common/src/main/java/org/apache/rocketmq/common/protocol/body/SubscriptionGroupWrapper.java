package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SubscriptionGroupWrapper extends RemotingSerializable {
    private ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable =
            new ConcurrentHashMap<String, SubscriptionGroupConfig>(1024);
    private DataVersion dataVersion = new DataVersion();

    public ConcurrentMap<String, SubscriptionGroupConfig> getSubscriptionGroupTable() {
        return subscriptionGroupTable;
    }

    public void setSubscriptionGroupTable(
            ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroupTable) {
        this.subscriptionGroupTable = subscriptionGroupTable;
    }

    public DataVersion getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
}
