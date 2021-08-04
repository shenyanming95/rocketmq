package org.apache.rocketmq.tools.parse.commitlog;

import java.net.InetSocketAddress;
import java.util.Arrays;

/**
 * @author shenyanming
 * Create on 2021/08/03 20:06
 */
public class CommitLogMessage {
    private int totalSize;
    private int magicCode;
    private int bodyCyc;
    private int queueId;
    private int flag;
    private long queueOffset;
    private long physicalOffset;
    private int sysFlag;
    private long bornTimeStamp;
    private InetSocketAddress bornHost;
    private long storeTimeStamp;
    private InetSocketAddress storeHost;
    private int reconsumeTimes;
    private long preparedTransactionOffset;
    private int bodyLength;
    private byte[] body;
    private byte topicLength;
    private byte[] topic;
    private short propertiesLength;
    private byte[] propertiesData;

    public int getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(int totalSize) {
        this.totalSize = totalSize;
    }

    public int getMagicCode() {
        return magicCode;
    }

    public void setMagicCode(int magicCode) {
        this.magicCode = magicCode;
    }

    public int getBodyCyc() {
        return bodyCyc;
    }

    public void setBodyCyc(int bodyCyc) {
        this.bodyCyc = bodyCyc;
    }

    public int getQueueId() {
        return queueId;
    }

    public void setQueueId(int queueId) {
        this.queueId = queueId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public long getQueueOffset() {
        return queueOffset;
    }

    public void setQueueOffset(long queueOffset) {
        this.queueOffset = queueOffset;
    }

    public long getPhysicalOffset() {
        return physicalOffset;
    }

    public void setPhysicalOffset(long physicalOffset) {
        this.physicalOffset = physicalOffset;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public void setSysFlag(int sysFlag) {
        this.sysFlag = sysFlag;
    }

    public long getBornTimeStamp() {
        return bornTimeStamp;
    }

    public void setBornTimeStamp(long bornTimeStamp) {
        this.bornTimeStamp = bornTimeStamp;
    }

    public InetSocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(InetSocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public long getStoreTimeStamp() {
        return storeTimeStamp;
    }

    public void setStoreTimeStamp(long storeTimeStamp) {
        this.storeTimeStamp = storeTimeStamp;
    }

    public InetSocketAddress getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(InetSocketAddress storeHost) {
        this.storeHost = storeHost;
    }

    public int getReconsumeTimes() {
        return reconsumeTimes;
    }

    public void setReconsumeTimes(int reconsumeTimes) {
        this.reconsumeTimes = reconsumeTimes;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public void setPreparedTransactionOffset(long preparedTransactionOffset) {
        this.preparedTransactionOffset = preparedTransactionOffset;
    }

    public int getBodyLength() {
        return bodyLength;
    }

    public void setBodyLength(int bodyLength) {
        this.bodyLength = bodyLength;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public byte getTopicLength() {
        return topicLength;
    }

    public void setTopicLength(byte topicLength) {
        this.topicLength = topicLength;
    }

    public byte[] getTopic() {
        return topic;
    }

    public void setTopic(byte[] topic) {
        this.topic = topic;
    }

    public short getPropertiesLength() {
        return propertiesLength;
    }

    public void setPropertiesLength(short propertiesLength) {
        this.propertiesLength = propertiesLength;
    }

    public byte[] getPropertiesData() {
        return propertiesData;
    }

    public void setPropertiesData(byte[] propertiesData) {
        this.propertiesData = propertiesData;
    }

    @Override
    public String toString() {
        return "CommitLogMessage{" +
                "totalSize=" + totalSize +
                ", magicCode=" + magicCode +
                ", bodyCyc=" + bodyCyc +
                ", queueId=" + queueId +
                ", flag=" + flag +
                ", queueOffset=" + queueOffset +
                ", physicalOffset=" + physicalOffset +
                ", sysFlag=" + sysFlag +
                ", bornTimeStamp=" + bornTimeStamp +
                ", bornHost=" + bornHost +
                ", storeTimeStamp=" + storeTimeStamp +
                ", storeHost=" + storeHost +
                ", reconsumeTimes=" + reconsumeTimes +
                ", preparedTransactionOffset=" + preparedTransactionOffset +
                ", bodyLength=" + bodyLength +
                ", body=" + new String(body) +
                ", topicLength=" + topicLength +
                ", topic=" + new String(topic) +
                ", propertiesLength=" + propertiesLength +
                ", propertiesData=" + new String(propertiesData) +
                '}';
    }
}
