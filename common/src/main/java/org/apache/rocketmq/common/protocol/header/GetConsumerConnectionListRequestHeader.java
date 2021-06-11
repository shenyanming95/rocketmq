package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetConsumerConnectionListRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;

    @Override
    public void checkFields() throws RemotingCommandException {
        // To change body of implemented methods use File | Settings | File
        // Templates.
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }
}
