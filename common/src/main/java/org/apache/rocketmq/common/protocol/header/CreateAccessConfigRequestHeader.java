package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CreateAccessConfigRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private String defaultTopicPerm;

    private String defaultGroupPerm;

    // list string,eg: topicA=DENY,topicD=SUB
    private String topicPerms;

    // list string,eg: groupD=DENY,groupD=SUB
    private String groupPerms;


    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    public String getWhiteRemoteAddress() {
        return whiteRemoteAddress;
    }

    public void setWhiteRemoteAddress(String whiteRemoteAddress) {
        this.whiteRemoteAddress = whiteRemoteAddress;
    }

    public boolean isAdmin() {
        return admin;
    }

    public void setAdmin(boolean admin) {
        this.admin = admin;
    }

    public String getDefaultTopicPerm() {
        return defaultTopicPerm;
    }

    public void setDefaultTopicPerm(String defaultTopicPerm) {
        this.defaultTopicPerm = defaultTopicPerm;
    }

    public String getDefaultGroupPerm() {
        return defaultGroupPerm;
    }

    public void setDefaultGroupPerm(String defaultGroupPerm) {
        this.defaultGroupPerm = defaultGroupPerm;
    }

    public String getTopicPerms() {
        return topicPerms;
    }

    public void setTopicPerms(String topicPerms) {
        this.topicPerms = topicPerms;
    }

    public String getGroupPerms() {
        return groupPerms;
    }

    public void setGroupPerms(String groupPerms) {
        this.groupPerms = groupPerms;
    }
}
