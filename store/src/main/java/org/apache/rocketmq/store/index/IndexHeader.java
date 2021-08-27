package org.apache.rocketmq.store.index;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link IndexFile}文件头, 主要存储六个信息
 *
 *
 *
 */
public class IndexHeader {
    /**
     * indexHeader 一共40个字节.
     */
    public static final int INDEX_HEADER_SIZE = 40;

    /**
     * indexHeader的6个字段的存储位置, 分别对应：
     * 1)、第一次存放消息的存储时间、
     * 2)、最后一次存储消息的存储时间、
     * 3)、起始物理偏移量、
     * 4)、终止物理偏移量、
     * 5)、hash slot使用数、
     * 6)、消息的个数
     */
    private static int beginTimestampIndex = 0;
    private static int endTimestampIndex = 8;
    private static int beginPhyoffsetIndex = 16;
    private static int endPhyoffsetIndex = 24;
    private static int hashSlotcountIndex = 32;
    private static int indexCountIndex = 36;

    /**
     * {@link IndexFile}的内存映射
     */
    private final ByteBuffer byteBuffer;

    /**
     * indexHeader的6个字段的存储缓冲区, 分别对应：
     * 1)、第一次存放消息的存储时间、
     * 2)、最后一次存储消息的存储时间、
     * 3)、起始物理偏移量、
     * 4)、终止物理偏移量、
     * 5)、hash slot使用数、
     * 6)、消息的个数
     */
    private AtomicLong beginTimestamp = new AtomicLong(0);
    private AtomicLong endTimestamp = new AtomicLong(0);
    private AtomicLong beginPhyOffset = new AtomicLong(0);
    private AtomicLong endPhyOffset = new AtomicLong(0);
    private AtomicInteger hashSlotCount = new AtomicInteger(0);
    private AtomicInteger indexCount = new AtomicInteger(1);

    public IndexHeader(final ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    /**
     * 加载{@link IndexHeader}到内存中
     */
    public void load() {
        // 加载 indexFile 前40个字节, 分别读取到6个变量中
        this.beginTimestamp.set(byteBuffer.getLong(beginTimestampIndex));
        this.endTimestamp.set(byteBuffer.getLong(endTimestampIndex));
        this.beginPhyOffset.set(byteBuffer.getLong(beginPhyoffsetIndex));
        this.endPhyOffset.set(byteBuffer.getLong(endPhyoffsetIndex));
        this.hashSlotCount.set(byteBuffer.getInt(hashSlotcountIndex));
        this.indexCount.set(byteBuffer.getInt(indexCountIndex));
        // 默认消息个数为1
        if (this.indexCount.get() <= 0) {
            this.indexCount.set(1);
        }
    }

    /**
     * 将所有数据更新到内存映射{@link java.nio.MappedByteBuffer}中
     */
    public void updateByteBuffer() {
        this.byteBuffer.putLong(beginTimestampIndex, this.beginTimestamp.get());
        this.byteBuffer.putLong(endTimestampIndex, this.endTimestamp.get());
        this.byteBuffer.putLong(beginPhyoffsetIndex, this.beginPhyOffset.get());
        this.byteBuffer.putLong(endPhyoffsetIndex, this.endPhyOffset.get());
        this.byteBuffer.putInt(hashSlotcountIndex, this.hashSlotCount.get());
        this.byteBuffer.putInt(indexCountIndex, this.indexCount.get());
    }

    /*
     * 6个变量的getter和setter方法.
     * 每次调用set()方法时, 都会相应地把数据写到内存映射中.
     */

    public long getBeginTimestamp() {
        return beginTimestamp.get();
    }

    public void setBeginTimestamp(long beginTimestamp) {
        this.beginTimestamp.set(beginTimestamp);
        this.byteBuffer.putLong(beginTimestampIndex, beginTimestamp);
    }

    public long getEndTimestamp() {
        return endTimestamp.get();
    }

    public void setEndTimestamp(long endTimestamp) {
        this.endTimestamp.set(endTimestamp);
        this.byteBuffer.putLong(endTimestampIndex, endTimestamp);
    }

    public long getBeginPhyOffset() {
        return beginPhyOffset.get();
    }

    public void setBeginPhyOffset(long beginPhyOffset) {
        this.beginPhyOffset.set(beginPhyOffset);
        this.byteBuffer.putLong(beginPhyoffsetIndex, beginPhyOffset);
    }

    public long getEndPhyOffset() {
        return endPhyOffset.get();
    }

    public void setEndPhyOffset(long endPhyOffset) {
        this.endPhyOffset.set(endPhyOffset);
        this.byteBuffer.putLong(endPhyoffsetIndex, endPhyOffset);
    }

    public AtomicInteger getHashSlotCount() {
        return hashSlotCount;
    }

    public void incHashSlotCount() {
        int value = this.hashSlotCount.incrementAndGet();
        this.byteBuffer.putInt(hashSlotcountIndex, value);
    }

    public int getIndexCount() {
        return indexCount.get();
    }

    public void incIndexCount() {
        int value = this.indexCount.incrementAndGet();
        this.byteBuffer.putInt(indexCountIndex, value);
    }
}
