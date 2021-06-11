package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetConsumerRunningInfoRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String consumerGroup;
    @CFNotNull
    private String clientId;
    @CFNullable
    private boolean jstackEnable;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isJstackEnable() {
        return jstackEnable;
    }

    public void setJstackEnable(boolean jstackEnable) {
        this.jstackEnable = jstackEnable;
    }
}
