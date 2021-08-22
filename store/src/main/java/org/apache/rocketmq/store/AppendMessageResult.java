package org.apache.rocketmq.store;

/**
 * 向 commit log 写入消息时, 返回结果
 */
public class AppendMessageResult {
    /**
     * 执行结果状态
     */
    private AppendMessageStatus status;

    /**
     * 从哪里开始写
     */
    private long wroteOffset;

    /**
     * 写入的字节数
     */
    private int wroteBytes;

    /**
     * 消息标识
     */
    private String msgId;

    /**
     * 消息存储的时间戳
     */
    private long storeTimestamp;

    /**
     *  consumer queue 偏移量(这个值是累加1的)
     */
    private long logicsOffset;

    /**
     * 记录写入到缓冲区的时间.
     * 在实际代码中, 消息要么写入到{@link MappedFile#getMappedByteBuffer()}, 要么写入到{@link MappedFile#writeBuffer},
     * 这个参数就是记录添加到上述缓冲区的时间.
     */
    private long pagecacheRT = 0;

    /**
     * 当处理批量消息时, 用来记录已处理的消息个数
     */
    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes,
                               String msgId, long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    public long getPagecacheRT() {
        return pagecacheRT;
    }

    public void setPagecacheRT(final long pagecacheRT) {
        this.pagecacheRT = pagecacheRT;
    }

    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

    public AppendMessageStatus getStatus() {
        return status;
    }

    public void setStatus(AppendMessageStatus status) {
        this.status = status;
    }

    public long getWroteOffset() {
        return wroteOffset;
    }

    public void setWroteOffset(long wroteOffset) {
        this.wroteOffset = wroteOffset;
    }

    public int getWroteBytes() {
        return wroteBytes;
    }

    public void setWroteBytes(int wroteBytes) {
        this.wroteBytes = wroteBytes;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public long getLogicsOffset() {
        return logicsOffset;
    }

    public void setLogicsOffset(long logicsOffset) {
        this.logicsOffset = logicsOffset;
    }

    public int getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(int msgNum) {
        this.msgNum = msgNum;
    }

    @Override
    public String toString() {
        return "AppendMessageResult{" + "status=" + status + ", wroteOffset=" + wroteOffset + ", " +
                "wroteBytes=" + wroteBytes + ", msgId='" + msgId + '\'' + ", storeTimestamp=" + storeTimestamp +
                ", logicsOffset=" + logicsOffset + ", pagecacheRT=" + pagecacheRT + ", msgNum=" + msgNum + '}';
    }
}
