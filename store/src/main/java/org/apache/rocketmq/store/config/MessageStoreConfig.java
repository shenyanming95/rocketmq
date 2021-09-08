package org.apache.rocketmq.store.config;

import org.apache.rocketmq.common.annotation.ImportantField;
import org.apache.rocketmq.store.*;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.index.IndexFile;

import java.io.File;
import java.nio.ByteBuffer;

public class MessageStoreConfig {

    /**
     * rocketMQ 保存日志数据的根目录, 其它文件目录都是在这个目录之下
     */
    @ImportantField
    private String storePathRootDir = System.getProperty("user.dir") + File.separator + "workdir" + File.separator + "store";

    /**
     * rocketMQ会将所有消息都存储到 commit log中, 这个路径就是 commit log 的存储目录
     */
    @ImportantField
    private String storePathCommitLog = System.getProperty("user.dir") + File.separator + "workdir" + File.separator + "store" + File.separator + "commitlog";

    /**
     * commit log 文件大小, 默认 1G
     */
    private int mappedFileSizeCommitLog = 1024 * 1024 * 1024;

    // ConsumeQueue file size,default is 30W
    private int mappedFileSizeConsumeQueue = 300000 * ConsumeQueue.CQ_STORE_UNIT_SIZE;

    // enable consume queue ext
    private boolean enableConsumeQueueExt = false;

    // ConsumeQueue extend file size, 48M
    private int mappedFileSizeConsumeQueueExt = 48 * 1024 * 1024;

    // Bit count of filter bit map.
    // this will be set by pipe of calculate filter bit map.
    private int bitMapLengthConsumeQueueExt = 64;

    /**
     * 执行刷盘操作的时间间隔
     * (tip: 刷盘即flush, 表示将数据保存到磁盘)
     */
    @ImportantField
    private int flushIntervalCommitLog = 500;

    /**
     * 仅仅在{@link #transientStorePoolEnable}设置为true, 即开启{@link TransientStorePool}情况下使用.
     * 指定执行 commit 操作的时间间隔, 单位取决于调用方设置的时间单位, 不唯一.
     * (tip: commit是指将数据从DirectByteBuffer刷入到FileChannel中)
     */
    @ImportantField
    private int commitIntervalCommitLog = 200;

    /**
     * 从 4.0.x 开始引入, 指定{@link PutMessageLock}的实现方式, 默认false表示使用自旋锁{@link PutMessageSpinLock},
     * 如果改为true, 则会使用互斥锁{@link PutMessageReentrantLock}
     */
    private boolean useReentrantLockWhenPutMessage = false;

    /**
     * 是否定时执行 flush 操作, 默认为false, 表示实时刷盘.
     * 这个参数会结合{@link #flushIntervalCommitLog}使用, 在执行刷盘操作时：
     * 1.若为true, 那么直接 thread.sleep() 一段时间后再执行 flush;
     * 2.若为false, 那么会 ServiceThread.waitForRunning() 一段时间后再执行 flush.
     */
    @ImportantField
    private boolean flushCommitLogTimed = false;

    // ConsumeQueue flush interval
    private int flushIntervalConsumeQueue = 1000;

    // Resource reclaim interval
    private int cleanResourceInterval = 10000;

    // CommitLog removal interval
    private int deleteCommitLogFilesInterval = 100;

    // ConsumeQueue removal interval
    private int deleteConsumeQueueFilesInterval = 100;
    private int destroyMapedFileIntervalForcibly = 1000 * 120;
    private int redeleteHangedFileInterval = 1000 * 120;

    // When to delete,default is at 4 am
    @ImportantField
    private String deleteWhen = "04";
    private int diskMaxUsedSpaceRatio = 75;

    // The number of hours to keep a log file before deleting it (in hours)
    @ImportantField
    private int fileReservedTime = 72;

    // Flow control for ConsumeQueue
    private int putMsgIndexHightWater = 600000;

    // The maximum size of message,default is 4M
    /**
     * 存储到 commit log 文件中每一条消息的最大值(包含了rocketMQ自身定义的消息数据)
     * 默认4M
     */
    private int maxMessageSize = 1024 * 1024 * 4;

    // Whether check the CRC32 of the records consumed.
    // This ensures no on-the-wire or on-disk corruption to the messages occurred.
    // This check adds some overhead,so it may be disabled in cases seeking extreme performance.
    /**
     * 文件恢复时是否校验CRC.
     * 可确保不会对发生的消息进行在线或磁盘损坏, 但是检查会增加一些开销, 在寻求极端性能的情况下可以选择禁用它
     */
    private boolean checkCRCOnRecover = true;

    /**
     * 一次flush至少需要脏页的数量, 针对commitlog文件, 默认4页
     */
    private int flushCommitLogLeastPages = 4;

    /**
     * 一次commit至少需要脏页的数量, 针对commitlog文件, 默认4页.
     * 若值改为0, 表示不限制
     */
    private int commitCommitLogLeastPages = 4;

    /**
     * rocketMQ在创建磁盘文件的时候, 会一次性按照{@link MappedFile#getFileSize()}用字节0填充整个文件.
     * 这个参数用来指定多少页 flush 一次, 默认是4096页.
     */
    private int flushLeastPagesWhenWarmMapedFile = 1024 / 4 * 16;

    /**
     * 一次 flush 至少需要的脏页数量, 针对 consumer queue 文件, 默认2页
     */
    private int flushConsumeQueueLeastPages = 2;

    /**
     * commitlog连续两次刷盘的最大间隔, 如果超过该间隔, 将忽略{@link #commitCommitLogLeastPages}限制
     */
    private int flushCommitLogThoroughInterval = 1000 * 10;

    /**
     * Commitlog连续两次提交的最大间隔, 如果超过该间隔, 将忽略{@link #commitCommitLogLeastPages}限制
     */
    private int commitCommitLogThoroughInterval = 200;

    private int flushConsumeQueueThoroughInterval = 1000 * 60;
    @ImportantField
    private int maxTransferBytesOnMessageInMemory = 1024 * 256;
    @ImportantField
    private int maxTransferCountOnMessageInMemory = 32;
    @ImportantField
    private int maxTransferBytesOnMessageInDisk = 1024 * 64;
    @ImportantField
    private int maxTransferCountOnMessageInDisk = 8;
    @ImportantField
    private int accessMessageInMemoryMaxRatio = 40;

    /**
     * 是否支持消息索引文件
     */
    @ImportantField
    private boolean messageIndexEnable = true;

    /**
     * {@link IndexFile}使用, hash槽的个数, 默认为五百万
     */
    private int maxHashSlotNum = 5000000;

    /**
     * {@link IndexFile}使用, 消息题目的个数, 默认为两千万
     */
    private int maxIndexNum = 5000000 * 4;

    private int maxMsgsNumBatch = 64;

    /**
     * 消息索引是否安全, 默认为 false.
     * 文件恢复时选择文件检测点（commitlog.consumeque）的最小的与文件最后更新对比, 如果为true, 文件恢复时选择文件检测点保存的索引更新时间作为对比
     */
    @ImportantField
    private boolean messageIndexSafe = false;

    /**
     * HA主从同步, master监听的端口地址, 用来接受slave连接
     */
    private int haListenPort = 10912;

    /**
     * Master与Slave心跳包发送间隔, 默认5s
     */
    private int haSendHeartbeatInterval = 1000 * 5;

    /**
     * Master与slave长连接空闲时间, 超过该时间将关闭连接, 单位毫秒
     */
    private int haHousekeepingInterval = 1000 * 20;

    /**
     * 一次HA主从同步传输的最大字节长度, 默认为32K
     */
    private int haTransferBatchSize = 1024 * 32;

    @ImportantField
    private String haMasterAddress = null;

    /**
     * 允许slave与master同步commitlog的最大字节数偏差, 默认256M.
     * 如果超过该值则表示该slave不可用
     */
    private int haSlaveFallbehindMax = 1024 * 1024 * 256;

    @ImportantField
    private BrokerRole brokerRole = BrokerRole.ASYNC_MASTER;

    /**
     * 刷盘方式, 一般是异步刷盘.
     */
    @ImportantField
    private FlushDiskType flushDiskType = FlushDiskType.ASYNC_FLUSH;

    /**
     * 同步刷盘的最大等待时间
     */
    private int syncFlushTimeout = 1000 * 5;

    /**
     * 延迟队列等级（s=秒，m=分，h=小时)
     */
    private String messageDelayLevel = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";

    /**
     * 延迟队列拉取进度刷盘间隔, 默认10s
     */
    private long flushDelayOffsetInterval = 1000 * 10;

    @ImportantField
    private boolean cleanFileForciblyEnable = true;

    /**
     * 是否要开启预写{@link org.apache.rocketmq.store.MappedFile}文件, true-开启
     * {@link AllocateMappedFileService}
     */
    private boolean warmMapedFileEnable = false;


    private boolean offsetCheckInSlave = false;
    private boolean debugLockEnable = false;

    /**
     * 是否允许重复复制, 默认为 false
     */
    private boolean duplicationEnable = false;

    private boolean diskFallRecorded = true;
    private long osPageCacheBusyTimeOutMills = 1000;
    private int defaultQueryMaxNum = 32;

    /**
     * 如果置为true, 会使用{@link org.apache.rocketmq.store.TransientStorePool}.
     * 它会预先创建一些直接缓冲区以供后续使用.
     */
    @ImportantField
    private boolean transientStorePoolEnable = false;

    private int transientStorePoolSize = 5;

    /**
     * 从{@link TransientStorePool}获取{@link ByteBuffer}是否支持快速失败
     */
    private boolean fastFailIfNoBufferInStorePool = false;

    /**
     * 是否启动 DLedger, 即使用{@link DLedgerCommitLog}.
     * 是指一组相同名称的 Broker, 至少需要3个节点, 通过 Raft 自动选举出一个 Leader,
     * 其余节点 作为 Follower, 并在 Leader 和 Follower 之间复制数据以保证高可用.
     */
    private boolean enableDLegerCommitLog = false;

    /**
     * DLedger Raft Group的名字, 建议和 brokerName 保持一致
     */
    private String dLegerGroup;

    /**
     * DLedger Group 内各节点的端口信息, 同一个 Group 内的各个节点配置必须要保证一致
     */
    private String dLegerPeers;

    /**
     * 节点 id, 必须属于 dLegerPeers 中的一个, 同 Group 内各个节点要唯一
     */
    private String dLegerSelfId;

    private String preferredLeaderId;

    private boolean isEnableBatchPush = false;

    public boolean isDebugLockEnable() {
        return debugLockEnable;
    }

    public void setDebugLockEnable(final boolean debugLockEnable) {
        this.debugLockEnable = debugLockEnable;
    }

    public boolean isDuplicationEnable() {
        return duplicationEnable;
    }

    public void setDuplicationEnable(final boolean duplicationEnable) {
        this.duplicationEnable = duplicationEnable;
    }

    public long getOsPageCacheBusyTimeOutMills() {
        return osPageCacheBusyTimeOutMills;
    }

    public void setOsPageCacheBusyTimeOutMills(final long osPageCacheBusyTimeOutMills) {
        this.osPageCacheBusyTimeOutMills = osPageCacheBusyTimeOutMills;
    }

    public boolean isDiskFallRecorded() {
        return diskFallRecorded;
    }

    public void setDiskFallRecorded(final boolean diskFallRecorded) {
        this.diskFallRecorded = diskFallRecorded;
    }

    public boolean isWarmMapedFileEnable() {
        return warmMapedFileEnable;
    }

    public void setWarmMapedFileEnable(boolean warmMapedFileEnable) {
        this.warmMapedFileEnable = warmMapedFileEnable;
    }

    public int getMappedFileSizeCommitLog() {
        return mappedFileSizeCommitLog;
    }

    public void setMappedFileSizeCommitLog(int mappedFileSizeCommitLog) {
        this.mappedFileSizeCommitLog = mappedFileSizeCommitLog;
    }

    public int getMappedFileSizeConsumeQueue() {

        int factor = (int) Math.ceil(this.mappedFileSizeConsumeQueue / (ConsumeQueue.CQ_STORE_UNIT_SIZE * 1.0));
        return (int) (factor * ConsumeQueue.CQ_STORE_UNIT_SIZE);
    }

    public void setMappedFileSizeConsumeQueue(int mappedFileSizeConsumeQueue) {
        this.mappedFileSizeConsumeQueue = mappedFileSizeConsumeQueue;
    }

    public boolean isEnableConsumeQueueExt() {
        return enableConsumeQueueExt;
    }

    public void setEnableConsumeQueueExt(boolean enableConsumeQueueExt) {
        this.enableConsumeQueueExt = enableConsumeQueueExt;
    }

    public int getMappedFileSizeConsumeQueueExt() {
        return mappedFileSizeConsumeQueueExt;
    }

    public void setMappedFileSizeConsumeQueueExt(int mappedFileSizeConsumeQueueExt) {
        this.mappedFileSizeConsumeQueueExt = mappedFileSizeConsumeQueueExt;
    }

    public int getBitMapLengthConsumeQueueExt() {
        return bitMapLengthConsumeQueueExt;
    }

    public void setBitMapLengthConsumeQueueExt(int bitMapLengthConsumeQueueExt) {
        this.bitMapLengthConsumeQueueExt = bitMapLengthConsumeQueueExt;
    }

    public int getFlushIntervalCommitLog() {
        return flushIntervalCommitLog;
    }

    public void setFlushIntervalCommitLog(int flushIntervalCommitLog) {
        this.flushIntervalCommitLog = flushIntervalCommitLog;
    }

    public int getFlushIntervalConsumeQueue() {
        return flushIntervalConsumeQueue;
    }

    public void setFlushIntervalConsumeQueue(int flushIntervalConsumeQueue) {
        this.flushIntervalConsumeQueue = flushIntervalConsumeQueue;
    }

    public int getPutMsgIndexHightWater() {
        return putMsgIndexHightWater;
    }

    public void setPutMsgIndexHightWater(int putMsgIndexHightWater) {
        this.putMsgIndexHightWater = putMsgIndexHightWater;
    }

    public int getCleanResourceInterval() {
        return cleanResourceInterval;
    }

    public void setCleanResourceInterval(int cleanResourceInterval) {
        this.cleanResourceInterval = cleanResourceInterval;
    }

    public int getMaxMessageSize() {
        return maxMessageSize;
    }

    public void setMaxMessageSize(int maxMessageSize) {
        this.maxMessageSize = maxMessageSize;
    }

    public boolean isCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public boolean getCheckCRCOnRecover() {
        return checkCRCOnRecover;
    }

    public void setCheckCRCOnRecover(boolean checkCRCOnRecover) {
        this.checkCRCOnRecover = checkCRCOnRecover;
    }

    public String getStorePathCommitLog() {
        return storePathCommitLog;
    }

    public void setStorePathCommitLog(String storePathCommitLog) {
        this.storePathCommitLog = storePathCommitLog;
    }

    public String getDeleteWhen() {
        return deleteWhen;
    }

    public void setDeleteWhen(String deleteWhen) {
        this.deleteWhen = deleteWhen;
    }

    public int getDiskMaxUsedSpaceRatio() {
        if (this.diskMaxUsedSpaceRatio < 10) return 10;

        if (this.diskMaxUsedSpaceRatio > 95) return 95;

        return diskMaxUsedSpaceRatio;
    }

    public void setDiskMaxUsedSpaceRatio(int diskMaxUsedSpaceRatio) {
        this.diskMaxUsedSpaceRatio = diskMaxUsedSpaceRatio;
    }

    public int getDeleteCommitLogFilesInterval() {
        return deleteCommitLogFilesInterval;
    }

    public void setDeleteCommitLogFilesInterval(int deleteCommitLogFilesInterval) {
        this.deleteCommitLogFilesInterval = deleteCommitLogFilesInterval;
    }

    public int getDeleteConsumeQueueFilesInterval() {
        return deleteConsumeQueueFilesInterval;
    }

    public void setDeleteConsumeQueueFilesInterval(int deleteConsumeQueueFilesInterval) {
        this.deleteConsumeQueueFilesInterval = deleteConsumeQueueFilesInterval;
    }

    public int getMaxTransferBytesOnMessageInMemory() {
        return maxTransferBytesOnMessageInMemory;
    }

    public void setMaxTransferBytesOnMessageInMemory(int maxTransferBytesOnMessageInMemory) {
        this.maxTransferBytesOnMessageInMemory = maxTransferBytesOnMessageInMemory;
    }

    public int getMaxTransferCountOnMessageInMemory() {
        return maxTransferCountOnMessageInMemory;
    }

    public void setMaxTransferCountOnMessageInMemory(int maxTransferCountOnMessageInMemory) {
        this.maxTransferCountOnMessageInMemory = maxTransferCountOnMessageInMemory;
    }

    public int getMaxTransferBytesOnMessageInDisk() {
        return maxTransferBytesOnMessageInDisk;
    }

    public void setMaxTransferBytesOnMessageInDisk(int maxTransferBytesOnMessageInDisk) {
        this.maxTransferBytesOnMessageInDisk = maxTransferBytesOnMessageInDisk;
    }

    public int getMaxTransferCountOnMessageInDisk() {
        return maxTransferCountOnMessageInDisk;
    }

    public void setMaxTransferCountOnMessageInDisk(int maxTransferCountOnMessageInDisk) {
        this.maxTransferCountOnMessageInDisk = maxTransferCountOnMessageInDisk;
    }

    public int getFlushCommitLogLeastPages() {
        return flushCommitLogLeastPages;
    }

    public void setFlushCommitLogLeastPages(int flushCommitLogLeastPages) {
        this.flushCommitLogLeastPages = flushCommitLogLeastPages;
    }

    public int getFlushConsumeQueueLeastPages() {
        return flushConsumeQueueLeastPages;
    }

    public void setFlushConsumeQueueLeastPages(int flushConsumeQueueLeastPages) {
        this.flushConsumeQueueLeastPages = flushConsumeQueueLeastPages;
    }

    public int getFlushCommitLogThoroughInterval() {
        return flushCommitLogThoroughInterval;
    }

    public void setFlushCommitLogThoroughInterval(int flushCommitLogThoroughInterval) {
        this.flushCommitLogThoroughInterval = flushCommitLogThoroughInterval;
    }

    public int getFlushConsumeQueueThoroughInterval() {
        return flushConsumeQueueThoroughInterval;
    }

    public void setFlushConsumeQueueThoroughInterval(int flushConsumeQueueThoroughInterval) {
        this.flushConsumeQueueThoroughInterval = flushConsumeQueueThoroughInterval;
    }

    public int getDestroyMapedFileIntervalForcibly() {
        return destroyMapedFileIntervalForcibly;
    }

    public void setDestroyMapedFileIntervalForcibly(int destroyMapedFileIntervalForcibly) {
        this.destroyMapedFileIntervalForcibly = destroyMapedFileIntervalForcibly;
    }

    public int getFileReservedTime() {
        return fileReservedTime;
    }

    public void setFileReservedTime(int fileReservedTime) {
        this.fileReservedTime = fileReservedTime;
    }

    public int getRedeleteHangedFileInterval() {
        return redeleteHangedFileInterval;
    }

    public void setRedeleteHangedFileInterval(int redeleteHangedFileInterval) {
        this.redeleteHangedFileInterval = redeleteHangedFileInterval;
    }

    public int getAccessMessageInMemoryMaxRatio() {
        return accessMessageInMemoryMaxRatio;
    }

    public void setAccessMessageInMemoryMaxRatio(int accessMessageInMemoryMaxRatio) {
        this.accessMessageInMemoryMaxRatio = accessMessageInMemoryMaxRatio;
    }

    public boolean isMessageIndexEnable() {
        return messageIndexEnable;
    }

    public void setMessageIndexEnable(boolean messageIndexEnable) {
        this.messageIndexEnable = messageIndexEnable;
    }

    public int getMaxHashSlotNum() {
        return maxHashSlotNum;
    }

    public void setMaxHashSlotNum(int maxHashSlotNum) {
        this.maxHashSlotNum = maxHashSlotNum;
    }

    public int getMaxIndexNum() {
        return maxIndexNum;
    }

    public void setMaxIndexNum(int maxIndexNum) {
        this.maxIndexNum = maxIndexNum;
    }

    public int getMaxMsgsNumBatch() {
        return maxMsgsNumBatch;
    }

    public void setMaxMsgsNumBatch(int maxMsgsNumBatch) {
        this.maxMsgsNumBatch = maxMsgsNumBatch;
    }

    public int getHaListenPort() {
        return haListenPort;
    }

    public void setHaListenPort(int haListenPort) {
        this.haListenPort = haListenPort;
    }

    public int getHaSendHeartbeatInterval() {
        return haSendHeartbeatInterval;
    }

    public void setHaSendHeartbeatInterval(int haSendHeartbeatInterval) {
        this.haSendHeartbeatInterval = haSendHeartbeatInterval;
    }

    public int getHaHousekeepingInterval() {
        return haHousekeepingInterval;
    }

    public void setHaHousekeepingInterval(int haHousekeepingInterval) {
        this.haHousekeepingInterval = haHousekeepingInterval;
    }

    public BrokerRole getBrokerRole() {
        return brokerRole;
    }

    public void setBrokerRole(BrokerRole brokerRole) {
        this.brokerRole = brokerRole;
    }

    public void setBrokerRole(String brokerRole) {
        this.brokerRole = BrokerRole.valueOf(brokerRole);
    }

    public int getHaTransferBatchSize() {
        return haTransferBatchSize;
    }

    public void setHaTransferBatchSize(int haTransferBatchSize) {
        this.haTransferBatchSize = haTransferBatchSize;
    }

    public int getHaSlaveFallbehindMax() {
        return haSlaveFallbehindMax;
    }

    public void setHaSlaveFallbehindMax(int haSlaveFallbehindMax) {
        this.haSlaveFallbehindMax = haSlaveFallbehindMax;
    }

    public FlushDiskType getFlushDiskType() {
        return flushDiskType;
    }

    public void setFlushDiskType(FlushDiskType flushDiskType) {
        this.flushDiskType = flushDiskType;
    }

    public void setFlushDiskType(String type) {
        this.flushDiskType = FlushDiskType.valueOf(type);
    }

    public int getSyncFlushTimeout() {
        return syncFlushTimeout;
    }

    public void setSyncFlushTimeout(int syncFlushTimeout) {
        this.syncFlushTimeout = syncFlushTimeout;
    }

    public String getHaMasterAddress() {
        return haMasterAddress;
    }

    public void setHaMasterAddress(String haMasterAddress) {
        this.haMasterAddress = haMasterAddress;
    }

    public String getMessageDelayLevel() {
        return messageDelayLevel;
    }

    public void setMessageDelayLevel(String messageDelayLevel) {
        this.messageDelayLevel = messageDelayLevel;
    }

    public long getFlushDelayOffsetInterval() {
        return flushDelayOffsetInterval;
    }

    public void setFlushDelayOffsetInterval(long flushDelayOffsetInterval) {
        this.flushDelayOffsetInterval = flushDelayOffsetInterval;
    }

    public boolean isCleanFileForciblyEnable() {
        return cleanFileForciblyEnable;
    }

    public void setCleanFileForciblyEnable(boolean cleanFileForciblyEnable) {
        this.cleanFileForciblyEnable = cleanFileForciblyEnable;
    }

    public boolean isMessageIndexSafe() {
        return messageIndexSafe;
    }

    public void setMessageIndexSafe(boolean messageIndexSafe) {
        this.messageIndexSafe = messageIndexSafe;
    }

    public boolean isFlushCommitLogTimed() {
        return flushCommitLogTimed;
    }

    public void setFlushCommitLogTimed(boolean flushCommitLogTimed) {
        this.flushCommitLogTimed = flushCommitLogTimed;
    }

    public String getStorePathRootDir() {
        return storePathRootDir;
    }

    public void setStorePathRootDir(String storePathRootDir) {
        this.storePathRootDir = storePathRootDir;
    }

    public int getFlushLeastPagesWhenWarmMapedFile() {
        return flushLeastPagesWhenWarmMapedFile;
    }

    public void setFlushLeastPagesWhenWarmMapedFile(int flushLeastPagesWhenWarmMapedFile) {
        this.flushLeastPagesWhenWarmMapedFile = flushLeastPagesWhenWarmMapedFile;
    }

    public boolean isOffsetCheckInSlave() {
        return offsetCheckInSlave;
    }

    public void setOffsetCheckInSlave(boolean offsetCheckInSlave) {
        this.offsetCheckInSlave = offsetCheckInSlave;
    }

    public int getDefaultQueryMaxNum() {
        return defaultQueryMaxNum;
    }

    public void setDefaultQueryMaxNum(int defaultQueryMaxNum) {
        this.defaultQueryMaxNum = defaultQueryMaxNum;
    }

    /**
     * 开启transient commitLog 存储池功能({@link TransientStorePool}), 需要同时满足：
     * 1.{@link #transientStorePoolEnable}为true；
     * 2.刷盘方式为异步, {@link FlushDiskType#ASYNC_FLUSH}
     * 3.当前broker为master
     *
     * @return true-支持, false-不支持
     */
    public boolean isTransientStorePoolEnable() {
        return transientStorePoolEnable && FlushDiskType.ASYNC_FLUSH == getFlushDiskType() && BrokerRole.SLAVE != getBrokerRole();
    }

    public void setTransientStorePoolEnable(final boolean transientStorePoolEnable) {
        this.transientStorePoolEnable = transientStorePoolEnable;
    }

    public int getTransientStorePoolSize() {
        return transientStorePoolSize;
    }

    public void setTransientStorePoolSize(final int transientStorePoolSize) {
        this.transientStorePoolSize = transientStorePoolSize;
    }

    public int getCommitIntervalCommitLog() {
        return commitIntervalCommitLog;
    }

    public void setCommitIntervalCommitLog(final int commitIntervalCommitLog) {
        this.commitIntervalCommitLog = commitIntervalCommitLog;
    }

    public boolean isFastFailIfNoBufferInStorePool() {
        return fastFailIfNoBufferInStorePool;
    }

    public void setFastFailIfNoBufferInStorePool(final boolean fastFailIfNoBufferInStorePool) {
        this.fastFailIfNoBufferInStorePool = fastFailIfNoBufferInStorePool;
    }

    public boolean isUseReentrantLockWhenPutMessage() {
        return useReentrantLockWhenPutMessage;
    }

    public void setUseReentrantLockWhenPutMessage(final boolean useReentrantLockWhenPutMessage) {
        this.useReentrantLockWhenPutMessage = useReentrantLockWhenPutMessage;
    }

    public int getCommitCommitLogLeastPages() {
        return commitCommitLogLeastPages;
    }

    public void setCommitCommitLogLeastPages(final int commitCommitLogLeastPages) {
        this.commitCommitLogLeastPages = commitCommitLogLeastPages;
    }

    public int getCommitCommitLogThoroughInterval() {
        return commitCommitLogThoroughInterval;
    }

    public void setCommitCommitLogThoroughInterval(final int commitCommitLogThoroughInterval) {
        this.commitCommitLogThoroughInterval = commitCommitLogThoroughInterval;
    }

    public String getdLegerGroup() {
        return dLegerGroup;
    }

    public void setdLegerGroup(String dLegerGroup) {
        this.dLegerGroup = dLegerGroup;
    }

    public String getdLegerPeers() {
        return dLegerPeers;
    }

    public void setdLegerPeers(String dLegerPeers) {
        this.dLegerPeers = dLegerPeers;
    }

    public String getdLegerSelfId() {
        return dLegerSelfId;
    }

    public void setdLegerSelfId(String dLegerSelfId) {
        this.dLegerSelfId = dLegerSelfId;
    }

    public boolean isEnableDLegerCommitLog() {
        return enableDLegerCommitLog;
    }

    public void setEnableDLegerCommitLog(boolean enableDLegerCommitLog) {
        this.enableDLegerCommitLog = enableDLegerCommitLog;
    }

    public String getPreferredLeaderId() {
        return preferredLeaderId;
    }

    public void setPreferredLeaderId(String preferredLeaderId) {
        this.preferredLeaderId = preferredLeaderId;
    }

    public boolean isEnableBatchPush() {
        return isEnableBatchPush;
    }

    public void setEnableBatchPush(boolean enableBatchPush) {
        isEnableBatchPush = enableBatchPush;
    }
}
