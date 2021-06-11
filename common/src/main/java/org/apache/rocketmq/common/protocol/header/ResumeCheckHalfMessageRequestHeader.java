package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class ResumeCheckHalfMessageRequestHeader implements CommandCustomHeader {
    @CFNullable
    private String msgId;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    @Override
    public String toString() {
        return "ResumeCheckHalfMessageRequestHeader [msgId=" + msgId + "]";
    }
}
