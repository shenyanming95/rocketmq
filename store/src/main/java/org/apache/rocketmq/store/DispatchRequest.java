package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.index.IndexFile;

import java.util.Map;

/**
 * 在恢复 commitlog 文件时, 记录 commit log 的信息.
 * 用于构建其它存储文件使用, 比如{@link IndexFile}
 */
public class DispatchRequest {

    /**
     * 主题名
     */
    private final String topic;

    /**
     * 队列序号
     */
    private final int queueId;

    /**
     * 消息在 commit log 的物理偏移量(算上了{@link MappedFile#getFileFromOffset()})
     */
    private final long commitLogOffset;

    /**
     * 消息的标签值, 即{@link MessageConst#PROPERTY_TAGS}
     */
    private final long tagsCode;

    /**
     * 消息的存储时间
     */
    private final long storeTimestamp;

    /**
     * consumerQueue偏移量
     */
    private final long consumeQueueOffset;

    /**
     * 即{@link MessageConst#PROPERTY_KEYS}
     */
    private final String keys;

    private final boolean success;

    /**
     * 即{@link MessageConst#PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX}
     */
    private final String uniqKey;

    /**
     * 系统标志, {@link MessageSysFlag}
     */
    private final int sysFlag;

    /**
     * 事务消息相关
     */
    private final long preparedTransactionOffset;

    /**
     * 消息附带的额外属性
     */
    private final Map<String, String> propertiesMap;

    /**
     * 消息的总大小
     */
    private int msgSize;

    private byte[] bitMap;

    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    public DispatchRequest(final String topic, final int queueId, final long commitLogOffset, final int msgSize, final long tagsCode, final long storeTimestamp, final long consumeQueueOffset, final String keys, final String uniqKey, final int sysFlag, final long preparedTransactionOffset, final Map<String, String> propertiesMap) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getCommitLogOffset() {
        return commitLogOffset;
    }

    public int getMsgSize() {
        return msgSize;
    }

    public void setMsgSize(int msgSize) {
        this.msgSize = msgSize;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public long getConsumeQueueOffset() {
        return consumeQueueOffset;
    }

    public String getKeys() {
        return keys;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public int getSysFlag() {
        return sysFlag;
    }

    public long getPreparedTransactionOffset() {
        return preparedTransactionOffset;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getUniqKey() {
        return uniqKey;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public byte[] getBitMap() {
        return bitMap;
    }

    public void setBitMap(byte[] bitMap) {
        this.bitMap = bitMap;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }
}
