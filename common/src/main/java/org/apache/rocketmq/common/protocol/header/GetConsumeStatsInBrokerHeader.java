package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetConsumeStatsInBrokerHeader implements CommandCustomHeader {
    @CFNotNull
    private boolean isOrder;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public boolean isOrder() {
        return isOrder;
    }

    public void setIsOrder(boolean isOrder) {
        this.isOrder = isOrder;
    }
}
