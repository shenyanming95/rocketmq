package org.apache.rocketmq.client.impl;

public class FindBrokerResult {
    private final String brokerAddr;
    private final boolean slave;
    private final int brokerVersion;

    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = 0;
    }

    public FindBrokerResult(String brokerAddr, boolean slave, int brokerVersion) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = brokerVersion;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public boolean isSlave() {
        return slave;
    }

    public int getBrokerVersion() {
        return brokerVersion;
    }
}
