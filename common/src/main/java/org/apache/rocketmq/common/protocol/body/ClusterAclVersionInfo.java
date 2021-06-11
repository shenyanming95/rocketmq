package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class ClusterAclVersionInfo extends RemotingSerializable {

    private String brokerName;

    private String brokerAddr;

    private DataVersion aclConfigDataVersion;

    private String clusterName;

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }


    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public DataVersion getAclConfigDataVersion() {
        return aclConfigDataVersion;
    }

    public void setAclConfigDataVersion(DataVersion aclConfigDataVersion) {
        this.aclConfigDataVersion = aclConfigDataVersion;
    }
}
