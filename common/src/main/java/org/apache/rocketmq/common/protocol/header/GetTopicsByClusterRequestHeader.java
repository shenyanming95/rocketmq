package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetTopicsByClusterRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String cluster;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }
}
