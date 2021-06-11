package org.apache.rocketmq.common;

import java.util.List;

public class PlainAccessConfig {

    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private String defaultTopicPerm;

    private String defaultGroupPerm;

    private List<String> topicPerms;

    private List<String> groupPerms;

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

    public List<String> getTopicPerms() {
        return topicPerms;
    }

    public void setTopicPerms(List<String> topicPerms) {
        this.topicPerms = topicPerms;
    }

    public List<String> getGroupPerms() {
        return groupPerms;
    }

    public void setGroupPerms(List<String> groupPerms) {
        this.groupPerms = groupPerms;
    }
}
