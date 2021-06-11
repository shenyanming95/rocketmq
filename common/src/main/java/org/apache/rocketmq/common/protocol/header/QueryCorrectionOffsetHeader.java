/**
 * $Id: GetMinOffsetRequestHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryCorrectionOffsetHeader implements CommandCustomHeader {
    private String filterGroups;
    @CFNotNull
    private String compareGroup;
    @CFNotNull
    private String topic;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getFilterGroups() {
        return filterGroups;
    }

    public void setFilterGroups(String filterGroups) {
        this.filterGroups = filterGroups;
    }

    public String getCompareGroup() {
        return compareGroup;
    }

    public void setCompareGroup(String compareGroup) {
        this.compareGroup = compareGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }
}
