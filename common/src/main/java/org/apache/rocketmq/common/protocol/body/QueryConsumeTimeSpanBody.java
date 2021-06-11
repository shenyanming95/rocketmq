package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.List;

public class QueryConsumeTimeSpanBody extends RemotingSerializable {
    List<QueueTimeSpan> consumeTimeSpanSet = new ArrayList<QueueTimeSpan>();

    public List<QueueTimeSpan> getConsumeTimeSpanSet() {
        return consumeTimeSpanSet;
    }

    public void setConsumeTimeSpanSet(List<QueueTimeSpan> consumeTimeSpanSet) {
        this.consumeTimeSpanSet = consumeTimeSpanSet;
    }
}
