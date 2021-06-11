package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.List;

public class GetConsumerListByGroupResponseBody extends RemotingSerializable {
    private List<String> consumerIdList;

    public List<String> getConsumerIdList() {
        return consumerIdList;
    }

    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }
}
