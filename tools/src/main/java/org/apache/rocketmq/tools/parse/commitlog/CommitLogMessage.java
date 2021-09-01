package org.apache.rocketmq.tools.parse.commitlog;

import java.net.InetSocketAddress;

/**
 * rocketMQ的commit log消息条目的数据结构
 *
 * @author shenyanming
 * Create on 2021/08/03 20:06
 */
public class CommitLogMessage {

    private int totalSize;                  //消息总大小
    private int magicCode;                  //魔数, 标识用, 比如写到文件末尾
    private int bodyCyc;                    //消息体body的CRC校验和
    private int queueId;                    //消费队列consumerQueue的序号
    private int flag;                       //网络通信层标记
    private long queueOffset;               //消息在ConsumerQueue中的偏移量
    private long physicalOffset;            //消息存储在commitLog的绝对偏移量
    private int sysFlag;                    //系统标志
    private String bornTimeStamp;           //消息在来源方生成的时间戳
    private InetSocketAddress bornHost;     //消息来源方主机地址
    private String storeTimeStamp;          //消息在Broker的存储时间
    private InetSocketAddress storeHost;    //消息在Broker的主机地址
    private int reconsumeTimes;             //消息重试消费次数
    private long preparedTransactionOffset; //事务消息相关
    private int bodyLength;                 //消息体的大小
    private byte[] body;                    //消息体
    private byte topicLength;               //主题名大小
    private byte[] topic;                   //主题名
    private short propertiesLength;         //附加属性大小
    private byte[] propertiesData;          //附加属性值

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

    public String getBornTimeStamp() {
        return bornTimeStamp;
    }

    public void setBornTimeStamp(String bornTimeStamp) {
        this.bornTimeStamp = bornTimeStamp;
    }

    public InetSocketAddress getBornHost() {
        return bornHost;
    }

    public void setBornHost(InetSocketAddress bornHost) {
        this.bornHost = bornHost;
    }

    public String getStoreTimeStamp() {
        return storeTimeStamp;
    }

    public void setStoreTimeStamp(String storeTimeStamp) {
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
