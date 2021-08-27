package org.apache.rocketmq.store.index;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.List;

/**
 * rocketMQ 设计索引文件实现, 类似于Java中的{@link HashMap}, 其结构设计如下.
 * 一个{@link IndexFile}包含三个部分：
 * 1)、文件头, 即{@link IndexHeader}, 保存一些总的统计信息;
 * 2)、哈希槽, 默认五百万个, 每个哈希槽四个字节, 用来存储下面"索引条目"的位置;
 * 3)、索引条目, 实际存储消息信息, 默认两千万个, 每个条目20字节, 包含4字节的hashcode, 8字节的commitlog偏移量, 4字节的存储时间差, 4字节的前一个索引条目索引.
 *
 * <pre>
 *
 * 丨← IndexHeader →丨  ←   500W个hash槽  → 丨 ← 2000W个Index条目 → 丨
 *  --------------------------------------------------------------
 * 丨               丨   丨   丨   丨   丨   丨                     丨
 *  --------------------------------------------------------------
 *         ↓                ↓   ↓   ↓              ↓↓
 * {@link IndexHeader}    每个hash槽4个字节,        (每个条目)
 * 一共40字节, 存储统计信息   存储索引条目中消息的序号       丨 4字节  丨  8字节   丨  4字节 丨   4字节    丨
 *                                                   ------------------------------------------
 *                                                  丨hashcode丨phyOffset丨timeDif丨pre index no丨
 * </pre>                                            ------------------------------------------
 */
public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 一个hash槽占据4个字节
     */
    private static int hashSlotSize = 4;

    /**
     * 一个索引条目占据20个字节
     */
    private static int indexSize = 20;

    /**
     *
     */
    private static int invalidIndex = 0;

    /**
     * 与{@link #hashSlotSize}配合使用, 表示多少个hash槽
     */
    private final int hashSlotNum;

    /**
     * 与{@link #indexSize}配合使用, 表示有多少条消息
     */
    private final int indexNum;

    /**
     * 用于映射 indexFile 磁盘文件
     */
    private final MappedFile mappedFile;

    /**
     * indexFile文件的通道
     */
    private final FileChannel fileChannel;

    /**
     * indexFile文件的内存映射
     */
    private final MappedByteBuffer mappedByteBuffer;

    /**
     * 索引文件头
     */
    private final IndexHeader indexHeader;

    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum, final long endPhyOffset, final long endTimestamp) throws IOException {
        // 文件总大小：文件头大小 + hash槽数量*hash槽个数 + 索引条目数量*索引条目大小
        int fileTotalSize = IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;
        // 创建文件头
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            // 将 indexHeader 的所有数据保存到内存映射中, 即下面这个mappedByteBuffer
            this.indexHeader.updateByteBuffer();
            // 强制刷新到磁盘
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 保存索引
     *
     * @param key            消息的key
     * @param phyOffset      commit log 物理偏移量
     * @param storeTimestamp 存储时间
     * @return true-成功
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        // 保证索引文件还可以继续添加消息
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 根据消息key, 计算得到哈希值
            int keyHash = indexKeyHashMethod(key);
            // 用哈希值对总槽数求余, 计算这个消息应该存放在哪一个哈希槽
            int slotPos = keyHash % this.hashSlotNum;
            // 计算出这条消息实际落在索引文件的位置, 之前说过
            // indexFile 分为三个部分: 文件头、哈希槽、索引条目, 这边就是累加上文件头固定字节数和哈希槽位置, 算出这个哈希槽在索引文件的实际位置.
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize, false);

                // 从内存映射文件中读出该哈希槽存储的值, 它保存的是索引条目的索引值.
                // 这边有两种情况：1.默认值0, 说明这个槽未被使用; 2.该槽已经被使用, 即出现哈希冲突.
                // IndexFile 对哈希冲突的处理方式就是链表.
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // 哈希槽存储的是消息位于索引条目的序号, 所以序号的取值区间为：[0, indexHeader.getIndexCount()].
                // 如果出现越界, 那么哈希槽的值就等于默认值0
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 计算与第一条存入消息的时间差
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 转换为秒
                timeDiff = timeDiff / 1000;
                // 保证时间差的有效性
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 根据文件头目前维护的消息条数, 计算出该条消息实际要存放在磁盘文件的偏移量.
                // 计算公式：文件头大小 + 哈希槽大小 + 当前索引条目数 * 每条索引条目大小
                int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + this.indexHeader.getIndexCount() * indexSize;

                // 这边的存储方式就是索引条目的结构：消息key哈希值 + commit log物理偏移量 + 存储时间差 + 上一个索引条目的偏移量
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);                       //4字节
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);          //8字节
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);  //4字节
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);   //4字节
                // 设置哈希槽的值. 所以哈希槽的值实际存储的是消息的序号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                // 如果是第一条消息, 那就同步设置消息头的起始物理偏移量和起始存储时间戳
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                // 哈希槽的值为0, 说明这个哈希槽一开始没有被使用过, 所以这边将哈希槽的使用次数+1.
                // 当然如果哈希槽的值非0, 那么说明出现哈希冲突, 意味着要使用同一个哈希槽, 此时次数就不会加1.
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }

                /*
                 * 更新索引头的数据
                 */

                // 存储的消息个数+1
                this.indexHeader.incIndexCount();
                // 设置最大的commit log偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                // 设置最大的存储时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            // 索引文件已经写不下
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount() + "; index max num = " + this.indexNum);
        }
        return false;
    }

    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum, final long begin, final long end, boolean lock) {
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount() || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }

                        int absIndexPos = IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize + nextIndexToRead * indexSize;

                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;

                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }

                        if (prevIndexRead <= invalidIndex || prevIndexRead > this.indexHeader.getIndexCount() || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                this.mappedFile.release();
            }
        }
    }

    /**
     * 获取消息key的哈希值, 就是直接取绝对值, 如果溢出了就返回0
     *
     * @param key 消息键
     * @return 哈希值
     */
    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        // 值溢出返回0
        if (keyHashPositive < 0) keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

}
