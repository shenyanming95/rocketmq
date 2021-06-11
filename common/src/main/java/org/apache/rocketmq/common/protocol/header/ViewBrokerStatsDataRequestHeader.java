package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ViewBrokerStatsDataRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String statsName;
    @CFNotNull
    private String statsKey;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getStatsName() {
        return statsName;
    }

    public void setStatsName(String statsName) {
        this.statsName = statsName;
    }

    public String getStatsKey() {
        return statsKey;
    }

    public void setStatsKey(String statsKey) {
        this.statsKey = statsKey;
    }
}
