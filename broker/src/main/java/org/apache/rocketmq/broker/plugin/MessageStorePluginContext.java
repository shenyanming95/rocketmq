package org.apache.rocketmq.broker.plugin;

import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class MessageStorePluginContext {
    private MessageStoreConfig messageStoreConfig;
    private BrokerStatsManager brokerStatsManager;
    private MessageArrivingListener messageArrivingListener;
    private BrokerConfig brokerConfig;

    public MessageStorePluginContext(MessageStoreConfig messageStoreConfig, BrokerStatsManager brokerStatsManager, MessageArrivingListener messageArrivingListener, BrokerConfig brokerConfig) {
        super();
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    public MessageArrivingListener getMessageArrivingListener() {
        return messageArrivingListener;
    }

    public BrokerConfig getBrokerConfig() {
        return brokerConfig;
    }

}
