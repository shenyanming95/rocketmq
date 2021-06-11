/**
 * $Id: RegisterOrderTopicRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RegisterOrderTopicRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String topic;
    @CFNotNull
    private String orderTopicString;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getOrderTopicString() {
        return orderTopicString;
    }

    public void setOrderTopicString(String orderTopicString) {
        this.orderTopicString = orderTopicString;
    }
}
