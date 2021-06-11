package org.apache.rocketmq.common.protocol.header.filtersrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RegisterFilterServerResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private long brokerId;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public long getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(long brokerId) {
        this.brokerId = brokerId;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }
}
