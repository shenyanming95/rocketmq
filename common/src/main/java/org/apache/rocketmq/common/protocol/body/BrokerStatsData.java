package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class BrokerStatsData extends RemotingSerializable {

    private BrokerStatsItem statsMinute;

    private BrokerStatsItem statsHour;

    private BrokerStatsItem statsDay;

    public BrokerStatsItem getStatsMinute() {
        return statsMinute;
    }

    public void setStatsMinute(BrokerStatsItem statsMinute) {
        this.statsMinute = statsMinute;
    }

    public BrokerStatsItem getStatsHour() {
        return statsHour;
    }

    public void setStatsHour(BrokerStatsItem statsHour) {
        this.statsHour = statsHour;
    }

    public BrokerStatsItem getStatsDay() {
        return statsDay;
    }

    public void setStatsDay(BrokerStatsItem statsDay) {
        this.statsDay = statsDay;
    }
}
