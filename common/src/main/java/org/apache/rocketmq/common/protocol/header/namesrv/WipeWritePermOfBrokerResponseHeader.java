package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class WipeWritePermOfBrokerResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Integer wipeTopicCount;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Integer getWipeTopicCount() {
        return wipeTopicCount;
    }

    public void setWipeTopicCount(Integer wipeTopicCount) {
        this.wipeTopicCount = wipeTopicCount;
    }
}
