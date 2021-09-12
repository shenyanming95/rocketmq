package org.apache.rocketmq.store;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * 消息消费队列, rocketMQ将消息主体信息全部写入都{@link CommitLog}中, 如果要消费消息, 直接遍历commit log效率太低.
 * 因此设计了{@link ConsumeQueue}作为消费消息的索引, 它的存储结构分为三个方面：
 * 1)、保存了指定 topic 下的队列消息在{@link CommitLog}中的起始物理偏移量
 * 2)、消息大小
 * 3)、消息Tag的hashcode值
 * <p>
 * 默认消费队列存储在：${STORE_HOME}/consumerqueue/{topic_name}/{queue_id}/, 一个{@link ConsumeQueue}实例
 * 管理一个 queue_id 下的所有磁盘文件, 也即一个实例就表示一个 ${topic_name}/${queue_id} 目录.
 */
public class ConsumeQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 存储格式：8字节的commit log偏移量 + 4字节的消息长度 + 8字节的消息标签.
     * 每一个条目共20个字节.
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;

    /**
     * 存储引擎引用
     */
    private final DefaultMessageStore defaultMessageStore;

    /**
     * 管理 consumer queue 文件
     */
    private final MappedFileQueue mappedFileQueue;

    /**
     * 该 consumerqueue 文件属于哪个topic
     */
    private final String topic;

    /**
     * 该 consumerqueue 文件属于哪个queueId, 与{@link #topic}组合成为一个目录
     */
    private final int queueId;

    private final ByteBuffer byteBufferIndex;

    /**
     * 整个 consumerqueue 存储目录, 即${STORE_HOME}/consumerqueue/
     */
    private final String storePath;

    /**
     * 每个 consumerqueue 文件的大小, 默认是 6000000 字节, 折合5.72M
     */
    private final int mappedFileSize;

    /**
     * 表示这个 consumerqueue 维护的最大的 commit log 偏移量
     */
    private long maxPhysicOffset = -1;

    /**
     * 表示这个 consumerqueue 维护的最小的 commit log 偏移量
     */
    private volatile long minLogicOffset = 0;

    /**
     * {@link ConsumeQueue}的拓展, 记录一些不重要的信息.
     */
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(final String topic, final int queueId, final String storePath, final int mappedFileSize, final DefaultMessageStore defaultMessageStore) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.defaultMessageStore = defaultMessageStore;
        this.topic = topic;
        this.queueId = queueId;
        // 实际文件目录, /consumerqueue/${topic_name}/{queue_id}
        String queueDir = this.storePath + File.separator + topic + File.separator + queueId;
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        // 如果开启了 consumeQueueExt 功能, 同时会为其生成相应实体类
        if (defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            this.consumeQueueExt = new ConsumeQueueExt(topic, queueId, StorePathConfigHelper.getStorePathConsumeQueueExt(defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()), defaultMessageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(), defaultMessageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt());
        }
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        if (isExtReadEnable()) {
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }
        return result;
    }

    public void destroy() {
        this.maxPhysicOffset = -1;
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    /**
     * 重启 rocketMQ 时, 调用这个方法来恢复已写入到磁盘的 consumerqueue 文件
     */
    public void recover() {
        // 获取内存映射文件
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 默认从倒数第三个文件开始恢复, 不足三个文件的情况直接取第一个文件
            int index = mappedFiles.size() - 3;
            if (index < 0) index = 0;
            // 一个 consumerqueue 文件的大小, 默认5.72M
            int mappedFileSizeLogics = this.mappedFileSize;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

            // consumerqueue文件的命名方式跟commitlog类似, 都是 00000000000000000000 开始.
            // 这边先获取 consumerqueue 文件的起始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            // 这个参数用来在读取过程中累加数据大小使用
            long mappedFileOffset = 0;

            long maxExtAddr = 1;
            while (true) {

                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    // 读取一条数据：
                    // consumerqueue文件存储格式：8字节的commit log偏移量 + 4字节的消息长度 + 8字节的消息标签
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (offset >= 0 && size > 0) {
                        // 累加读到的数据
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        // 每次读取, 此 consumerqueue维护的最大commitlog物理偏移量, 等于
                        // 它存储的commitlog偏移量 + 原消息的大小
                        this.maxPhysicOffset = offset + size;
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        // 读到的数据小于0, 说明读完了, 跳出for循环
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " " + offset + " " + size + " " + tagsCode);
                        break;
                    }
                } // for循环终止

                // 一次for循环读取到的 consumerqueue 的数据, 正好等于consumerqueue配置的大小
                if (mappedFileOffset == mappedFileSizeLogics) {
                    // 换下一个文件读取
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 读到末尾了直接跳出while循环
                        log.info("recover last consume queue file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        // 没有读到末尾就切换下一个文件读取
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue queue over " + mappedFile.getFileName() + " " + (processOffset + mappedFileOffset));
                    break;
                }
            }
            // 所有数据都读取完以后, 累加读到的数据,
            processOffset += mappedFileOffset;
            // 重新赋值到 mappedFileQueue 中, 注意这个值是一个物理偏移量(算入了mappedFile)
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
            // 若开启扩展, 同时恢复扩展
            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    /**
     * consumerqueue文件根据时间戳查询消息, 使用的算法是二分搜索,
     * 不过这边的二分搜索是将一个 consumerqueue 数据体作为一个整体, 对其进行搜索
     *
     * @param timestamp 要匹配的时间戳
     * @return consumerqueue存储条目的偏移量
     */
    public long getOffsetInQueueByTime(final long timestamp) {
        // 获取第一个磁盘文件修改时间大于等于参数timestamp的 consumerqueue 文件.
        MappedFile mappedFile = this.mappedFileQueue.getMappedFileByTime(timestamp);
        // 如果文件为空返回0, 不为空进入下面的处理逻辑
        if (mappedFile != null) {

            // 返回值, consumerqueue的逻辑偏移量
            long offset = 0;

            // 确定搜索的最小边界、最大边界.
            // 如果consumerqueue文件的起始偏移量小于等于minLogicOffset, 那么直接从0开始搜索; 相反地, 如果大于起始偏移量, 那么减去起始偏移量作为最小边界
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;

            // 二分搜索的变量：中间位置、目标值、左边位置、右边位置
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;

            // 一旦未精确匹配到指定存储时间戳的消息, 会根据在二分搜索中的查询过程, 来选择一个兜底的消息返回.
            // 这两个参数就是用来存储每次决定向左搜索还是向右搜索时的边界值.
            long leftIndexValue = -1L, rightIndexValue = -1L;

            // 当前存储的最小commitlog偏移量
            long minPhysicOffset = this.defaultMessageStore.getMinPhyOffset();

            // 返回consumerqueue的可读数据大小
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0);

            // 开始搜索
            if (null != sbr) {
                // 待搜索的目标数据
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                // 确定访问的最大边界, 就是当前consumerqueue文件的最大存储条目的个数(consumerqueue固定是20字节的存储条目)
                high = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                try {
                    // 二分搜索的循环条件就是, 最大边界小于等于最小边界
                    while (high >= low) {
                        // rocketMQ是吧一个consumerqueue条目(20字节)当做一个整体.
                        // 实际上这边直接相当于 high + low / 2
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;

                        // 在buffer中定位到中间位置
                        byteBuffer.position(midOffset);

                        // consumerqueue存储条目：8字节的commit log偏移量 + 4字节的消息长度 + 8字节的消息标签.
                        long phyOffset = byteBuffer.getLong();
                        int size = byteBuffer.getInt();

                        // 比当前broker存储的commitlog偏移量还小, 那么二分搜索改为使用右半部分.
                        // 即 high 不变, middle 变 left。
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }

                        // 从commitlog中搜索出存储时间戳
                        long storeTime = this.defaultMessageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);

                        // 根据储存时间戳, 来决定二分搜索是向左边呢还是向右边呢.
                        if (storeTime < 0) {
                            return 0;
                        } else if (storeTime == timestamp) {
                            //匹配到指定存储时间戳的消息, 目标consumerqueue的偏移量就是midOffset
                            targetOffset = midOffset;
                            break;
                        } else if (storeTime > timestamp) {
                            // 查到的存储时间大于目标存储时间, 向左边找, low不变, high变为middle
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                            rightIndexValue = storeTime;
                        } else {
                            // 查到的存储时间小于目标存储时间, 往右边找, high不变, low变为middle
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            leftIndexValue = storeTime;
                        }
                    }

                    // 跳出循环, 要么找到了指定消息, 要么压根消息就不再consumerqueue中

                    if (targetOffset != -1) {
                        // targetOffset不等于-1, 说明精确匹配到时间戳
                        offset = targetOffset;
                    } else {
                        // targetOffset等于-1, 说明未匹配到时间戳, rocketMQ选择兜底操作.
                        // 就看看二分搜索过程中, 边界值的变化: 若确定向右遍历, 那么记录左边值; 若确定向左遍历, 那么记录右边值.
                        if (leftIndexValue == -1) {
                            offset = rightOffset;
                        } else if (rightIndexValue == -1) {
                            offset = leftOffset;
                        } else {
                            // 如果二分搜索过程, 即向左搜索过, 也向右搜索过.
                            // 那就看下哪个时间戳跟目标时间戳差别小, 选小的返回
                            offset = Math.abs(timestamp - leftIndexValue) > Math.abs(timestamp - rightIndexValue) ? rightOffset : leftOffset;
                        }
                    }
                    // 返回consumerqueue目标位置
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    public void truncateDirtyLogicFiles(long phyOffet) {

        int logicFileSize = this.mappedFileSize;

        this.maxPhysicOffset = phyOffet;
        long maxExtAddr = 1;
        while (true) {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);

                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        if (offset >= phyOffet) {
                            this.mappedFileQueue.deleteLastMappedFile();
                            break;
                        } else {
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {

                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffet) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.maxPhysicOffset = offset + size;
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }
            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0) position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    public void correctMinOffset(long phyMinOffset) {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(0);
            if (result != null) {
                try {
                    for (int i = 0; i < result.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offsetPy = result.getByteBuffer().getLong();
                        result.getByteBuffer().getInt();
                        long tagsCode = result.getByteBuffer().getLong();

                        if (offsetPy >= phyMinOffset) {
                            this.minLogicOffset = mappedFile.getFileFromOffset() + i;
                            log.info("Compute logical min offset: {}, topic: {}, queueId: {}", this.getMinOffsetInQueue(), this.topic, this.queueId);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                minExtAddr = tagsCode;
                            }
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Exception thrown when correctMinOffset", e);
                } finally {
                    result.release();
                }
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        final int maxRetries = 30;
        boolean canWrite = this.defaultMessageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            long tagsCode = request.getTagsCode();
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit, topic, queueId, request.getCommitLogOffset());
                }
            }
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            if (result) {
                if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE || this.defaultMessageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.defaultMessageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset() + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.defaultMessageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode, final long cqOffset) {

        if (offset + size <= this.maxPhysicOffset) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", maxPhysicOffset, offset);
            return true;
        }

        this.byteBufferIndex.flip();
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        this.byteBufferIndex.putLong(offset);
        this.byteBufferIndex.putInt(size);
        this.byteBufferIndex.putLong(tagsCode);

        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " " + mappedFile.getWrotePosition());
            }

            if (cqOffset != 0) {
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();

                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}", expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }

                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn("[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}", expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                }
            }
            this.maxPhysicOffset = offset + size;
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 获取 consumer queue 指定偏移量的数据
     *
     * @param startIndex 数据条目的开始索引, 拿它乘以 CQ_STORE_UNIT_SIZE 就可以等到实际物理偏移量
     * @return 查询数据
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        // 每个 consumer queue 20个字节, 这边乘以20就是拿到实际的物理偏移量
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        // 要查询的索引必须大于等于最小的逻辑偏移量
        if (offset >= this.getMinLogicOffset()) {
            // 查找目标偏移量存在于哪一个mappedFile上
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                // 直接从mappedFile中读取相应的数据
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    public long rollNextFile(final long index) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return index + totalUnitsInFile - index % totalUnitsInFile;
    }

    public String getTopic() {
        return topic;
    }

    public int getQueueId() {
        return queueId;
    }

    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null && this.defaultMessageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }
}
