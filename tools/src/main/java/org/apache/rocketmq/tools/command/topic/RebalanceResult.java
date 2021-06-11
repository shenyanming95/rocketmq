package org.apache.rocketmq.tools.command.topic;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RebalanceResult {
    private Map<String/*ip*/, List<MessageQueue>> result = new HashMap<String, List<MessageQueue>>();

    public Map<String, List<MessageQueue>> getResult() {
        return result;
    }

    public void setResult(final Map<String, List<MessageQueue>> result) {
        this.result = result;
    }
}
