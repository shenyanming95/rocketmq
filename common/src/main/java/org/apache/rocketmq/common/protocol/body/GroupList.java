package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;

public class GroupList extends RemotingSerializable {
    private HashSet<String> groupList = new HashSet<String>();

    public HashSet<String> getGroupList() {
        return groupList;
    }

    public void setGroupList(HashSet<String> groupList) {
        this.groupList = groupList;
    }
}
