package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetProducerConnectionListRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;

    @Override
    public void checkFields() throws RemotingCommandException {
        // To change body of implemented methods use File | Settings | File
        // Templates.
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }
}
