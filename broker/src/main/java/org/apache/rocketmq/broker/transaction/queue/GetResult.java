package org.apache.rocketmq.broker.transaction.queue;

import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.message.MessageExt;

public class GetResult {
    private MessageExt msg;
    private PullResult pullResult;

    public MessageExt getMsg() {
        return msg;
    }

    public void setMsg(MessageExt msg) {
        this.msg = msg;
    }

    public PullResult getPullResult() {
        return pullResult;
    }

    public void setPullResult(PullResult pullResult) {
        this.pullResult = pullResult;
    }
}
