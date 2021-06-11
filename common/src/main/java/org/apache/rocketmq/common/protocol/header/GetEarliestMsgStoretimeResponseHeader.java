/**
 * $Id: GetEarliestMsgStoretimeResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetEarliestMsgStoretimeResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Long timestamp;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
