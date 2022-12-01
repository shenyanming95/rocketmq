package org.apache.rocketmq.store;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.*;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 每个 broker 有且仅有一个 commitlog 文件, 所有 topic 的消息都会存入到这个 commitlog 文件中, 它的存储方式很有规律：
 * 文件名长度为20位, 左边补0. 比如：00000000000000000000代表了第一个文件, 起始偏移量为0, 文件大小为1G=1073741824;
 * 当第一个文件写满了, 第二个文件名为00000000001073741824, 起始偏移量为1073741824. 这样根据物理偏移量就能快速定位到消息.
 * <p>
 * 这个类中有3种刷盘实现：
 * 1.性能最高, 安全性最低, {@link CommitRealTimeService}, 它直接把消息写到{@link FileChannel}内就返回;
 * 2.性能居中, 安全性居中, {@link FlushRealTimeService}, 它通过{@link MappedByteBuffer#force()}, 异步地将消息刷盘;
 * 3.性能最慢, 安全性最高, {@link GroupCommitService}, 刷盘方式跟第2种一样, 只不过它需要同步等待{@link Future#get()}, 因此效果如同步刷盘一样.
 * <p>
 *
 * @see org.apache.rocketmq.tools.parse.commitlog.CommitLogMessage
 */
public class CommitLog {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 消息魔法代码(daa320a7), 跟 class 文件的 magic code 一个性质.
     */
    public final static int MESSAGE_MAGIC_CODE = -626843481;

    /**
     * 文件结尾空的魔法代码, cbd43194
     */
    protected final static int BLANK_MAGIC_CODE = -875286124;

    /**
     * 用来管理 commit log 文件集合
     */
    protected final MappedFileQueue mappedFileQueue;

    /**
     * 默认的消息引擎存储实现
     */
    protected final DefaultMessageStore defaultMessageStore;

    /**
     * rocketMQ 是以同步的方式写入 commit log 文件, 这个参数就是用来获取写入锁.
     */
    protected final PutMessageLock putMessageLock;

    /**
     * 执行 flush 操作的异步线程, 取决于{@link MessageStoreConfig#getFlushDiskType()}.
     * 异步刷盘使用：{@link FlushRealTimeService}, 同步刷盘使用{@link GroupCommitService}
     */
    private final FlushCommitLogService flushCommitLogService;

    /**
     * 执行 commit 操作的异步线程.
     * If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
     */
    private final FlushCommitLogService commitLogService;

    /**
     * 实际执行消息存储的逻辑{@link DefaultAppendMessageCallback}
     */
    private final AppendMessageCallback appendMessageCallback;

    /**
     * 用来存储{@link MessageExtBatchEncoder}, 因为 MessageExtBatchEncoder 是线程不安全的, rocketMQ
     * 将其保存到 ThreadLocal 中, 并且设置初始值, 每个线程会获取到独立的 MessageExtBatchEncoder, 而不用提前
     * 调用{@link ThreadLocal#set(Object)}.
     * <p>
     * {@link MessageExtBatchEncoder}会将{@link Message#getBody()}里面的批量消息解码到
     * {@link MessageExtBatch#getEncodedBuff()}中
     */
    private final ThreadLocal<MessageExtBatchEncoder> batchEncoderThreadLocal;

    /**
     * 消息存储的时刻(只要有消息成功存储了, 都会更新)
     */
    private volatile long beginTimeInLock = 0;

    /**
     * 用来维护消息在 ConsumerQueue 的存储偏移量, 初始值都为0, 后续存储一条commitlog消息成功, offset就会累加1.
     * 存储结构为：<topic+queueId, <offset>>
     */
    protected HashMap<String/* topic-queueid */, Long/* offset */> topicQueueTable = new HashMap<String, Long>(1024);

    protected volatile long confirmOffset = -1L;

    public CommitLog(final DefaultMessageStore defaultMessageStore) {
        MessageStoreConfig storeConfig = defaultMessageStore.getMessageStoreConfig();
        this.defaultMessageStore = defaultMessageStore;
        this.commitLogService = new CommitRealTimeService();
        this.putMessageLock = storeConfig.isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();
        this.appendMessageCallback = new DefaultAppendMessageCallback(storeConfig.getMaxMessageSize());
        // 创建 commit log 对应的 mappedFileQueue
        this.mappedFileQueue = new MappedFileQueue(storeConfig.getStorePathCommitLog(), storeConfig.getMappedFileSizeCommitLog(), defaultMessageStore.getAllocateMappedFileService());
        // 同步刷盘使用 GroupCommitService, 异步刷盘使用 FlushRealTimeService
        if (FlushDiskType.SYNC_FLUSH == storeConfig.getFlushDiskType()) {
            this.flushCommitLogService = new GroupCommitService();
        } else {
            this.flushCommitLogService = new FlushRealTimeService();
        }
        // initialValue()方法的作用就是每个线程调用 get() 获取对象时, 如果为空, 那就会调用 initialValue() 方法获取.
        // 就是省略了 ThreadLocal#set() 方法.
        this.batchEncoderThreadLocal = new ThreadLocal<MessageExtBatchEncoder>() {
            @Override
            protected MessageExtBatchEncoder initialValue() {
                return new MessageExtBatchEncoder(storeConfig.getMaxMessageSize());
            }
        };
    }

    /**
     * 用来计算, 最终要存储到磁盘的消息(包含元数据), 占用的字节数.
     *
     * @param sysFlag          系统标志, 用来确定是 IPV4 还是 IPV6
     * @param bodyLength       消息体(用户设置的消息内容)大小
     * @param topicLength      主题名称大小
     * @param propertiesLength 消息额外属性大小
     * @return 总的字节数
     */
    protected static int calMsgLength(int sysFlag, int bodyLength, int topicLength, int propertiesLength) {
        // 解释一下这边的意思：
        // IPV4地址是4个字节, IPV6地址是16个字节, 加上端口号4个字节(端口号是整数int, int在java中占用4个字节)
        // 所以如果当前主机用的是IPV4, 那么记录主机地址就需要4+4=8个字节; 同理, 如果用的是IPV6, 那么记录主机地址就需要16+4=20个字节.
        int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int storeHostAddressLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 8 : 20;

        // 计算存储这条消息需要用多少字节.
        // 4个字节一般是int, 8个字节一般是long, 2个字节一般是short, 1个字节一般是byte
        final int msgLen = 4 //TOTALSIZE-4个字节记录-消息的总大小
                + 4 //MAGICCODE-4个字节记录-魔数
                + 4 //BODYCRC-4个字节记录-消息体CRC值
                + 4 //QUEUEID-4个字节记录-队列号
                + 4 //FLAG-4个字节记录-网络通信层标记
                + 8 //QUEUEOFFSET-8个字节记录-消息在 ConsumerQueue 中的偏移量
                + 8 //PHYSICALOFFSET-8个字节记录-消息存储在commit log的绝对偏移量
                + 4 //SYSFLAG-4个字节记录-系统标志
                + 8 //BORNTIMESTAMP-8个字节记录-消息在来源方生成的时间戳
                + bornHostLength //BORNHOST-8或20的字节记录-消息来源方主机地址
                + 8 //STORETIMESTAMP-8个字节记录-消息存储的时间戳
                + storeHostAddressLength //STOREHOSTADDRESS-8或20个字节记录-存储消息的broker的主机地址
                + 4 //RECONSUMETIMES-4个字节记录-消息重试消费次数
                + 8 //Prepared Transaction Offset //8个字节记录-事务消息相关
                + 4 + (Math.max(bodyLength, 0)) //BODY-4个字节来记录-消息体的大小, 接着加上消息体自身需要的字节数
                + 1 + topicLength //TOPIC-1个字节记录-消息主题名称的大小, 接着加上主题名称自身需要的字节数
                + 2 + (Math.max(propertiesLength, 0)) //propertiesLength-2个字节记录-额外属性需要的大小, 接着加上额外属性自身需要的字节数
                + 0;
        return msgLen;
    }

    public boolean load() {
        boolean result = this.mappedFileQueue.load();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        // 启动刷盘线程, 两种实现：要么同步刷, 要么异步刷
        this.flushCommitLogService.start();
        // 更快速地刷盘策略, 需要满足一定条件才会开启
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.start();
        }
    }

    public void shutdown() {
        if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
            this.commitLogService.shutdown();
        }
        this.flushCommitLogService.shutdown();
    }

    public long flush() {
        // 简单明了, 直接先 commit 再 flush.
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(final long expiredTime, final int deleteFilesInterval, final long intervalForcibly, final boolean cleanImmediately) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately);
    }

    /**
     * 获取 commitlog 文件指定偏移量开始的有效数据
     *
     * @param offset 绝对地址的偏移量, 即计入了{@link MappedFile#getFileFromOffset()}
     * @return 数据结果
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 获取 commitlog 文件指定偏移量开始的有效数据
     *
     * @param offset                绝对地址的偏移量, 即计入了{@link MappedFile#getFileFromOffset()}
     * @param returnFirstOnNotFound 当找到offset所在的mappedFile时, 是否需要返回第一个文件
     * @return 数据结果
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        // 获取配置的 commitlog 文件的大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 通过 offset 定位消息位于哪一个 mappedFile 上
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            // offset 是绝对地址, 将其对 mappedFile 文件大小求余后, 就得到这个 offset 在 mappedFile 的逻辑地址.
            int pos = (int) (offset % mappedFileSize);
            // 获取数据集合
            return mappedFile.selectMappedBuffer(pos);
        }
        return null;
    }

    /**
     * 恢复磁盘文件数据, 将其更新回{@link MappedFile}的各个位置值中.
     * 如果 rocketMQ 上一次是正常退出, 调用此方法; 如果是异常退出, 调用{@link #recoverAbnormally(long)}
     *
     * @param maxPhyOffsetOfConsumeQueue consumerQueue 存储的最大commit log偏移量
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // 从倒数第三个文件开始解析
            int index = mappedFiles.size() - 3;
            if (index < 0) index = 0;
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            // 每个MappedFile的起始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            // 用来累加每一条消息的大小
            long mappedFileOffset = 0;
            // 开始解析
            while (true) {
                // 读取当前 MappedFile 中的每一条消息
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                // 消息的总大小：0,表示读到最后一条消息, >0,表示正常消息, -1,表示读取消息失败.
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess() && size > 0) { // 如果是正常消息, 便累加它的大小
                    mappedFileOffset += size;
                } else if (dispatchRequest.isSuccess() && size == 0) { // 当size==0表示已经读取到当前 MappedFile 的末尾, 此时需要切换到下一个MappedFile去解析.
                    index++;
                    if (index >= mappedFiles.size()) {
                        // 所有mappedFile已经读取完毕
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        // 由于切换了mappedFile文件, 所以重新赋值变量
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                } else if (!dispatchRequest.isSuccess()) { // Intermediate file read error
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }
            // while循环完就已经解析完所有 mappedFile, 将最后一个 mappedFile 的起始偏移量加上它有效消息的大小,
            // 就表示当前已经存入到磁盘的绝对偏移量, 最后调用 mappedFileQueue 去重置里面的位置变量.
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // consumerQueue 维护的commit log最大偏移量大于等于 commit log 自身的最大值, 那么就删除掉这些 consumer queue 文件
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    /**
     * 恢复磁盘文件数据, 将其更新回{@link MappedFile}的各个位置值中.
     * 如果 rocketMQ 上一次是异常退出, 调用此方法; 如果是正常退出, 调用{@link #recoverNormally(long)}
     *
     * @param maxPhyOffsetOfConsumeQueue consumerQueue 存储的最大commit log偏移量
     */
    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        // 正常关闭后的恢复逻辑是从倒数第三个文件开始恢复, 而异常关闭后的恢复逻辑是直接拿最后一个文件来恢复
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;

            // 从最后一个文件开始往前找, 直至找到能够用来恢复的 mappedFile.
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            // 越界则直接找第一个文件
            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                // 读取当前 MappedFile 中的每一条消息
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover);
                // 消息的总大小：0,表示读到最后一条消息, >0,表示正常消息, -1,表示读取消息失败.
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) { // 如果是正常消息, 便累加它的大小
                    if (size > 0) {
                        mappedFileOffset += size;
                        // 异常恢复时, 还需要重建 consumerQueue 和 indexFile
                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                            if (dispatchRequest.getCommitLogOffset() < this.defaultMessageStore.getConfirmOffset()) {
                                this.defaultMessageStore.doDispatch(dispatchRequest);
                            }
                        } else {
                            this.defaultMessageStore.doDispatch(dispatchRequest);
                        }
                    } else if (size == 0) { // 当size==0表示已经读取到当前 MappedFile 的末尾, 此时需要切换到下一个MappedFile去解析.
                        index++;
                        if (index >= mappedFiles.size()) {
                            // 所有mappedFile已经读取完毕
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            // 由于切换了mappedFile文件, 所以重新赋值变量
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else { //解析消息失败
                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }
            // while循环完就已经解析完所有 mappedFile, 将最后一个 mappedFile 的起始偏移量加上它有效消息的大小,
            // 就表示当前已经存入到磁盘的绝对偏移量, 最后调用 mappedFileQueue 去重置里面的位置变量.
            processOffset += mappedFileOffset;
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // consumerQueue 维护的commit log最大偏移量大于等于 commit log 自身的最大值, 那么就删除掉这些 consumer queue 文件
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    /**
     * 检查{@link ByteBuffer}中的一条消息(注意每调用一次这个, 只会解析出来一条消息)
     *
     * @param byteBuffer 内存数据
     * @param checkCRC   是否检查CRC
     * @return 解析结果, {@link DispatchRequest#getMsgSize()} 0 文件末尾, >0 正常消息, -1 消息读取失败
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, true);
    }

    /**
     * 检查{@link ByteBuffer}中的一条消息(注意每调用一次这个, 只会解析出来一条消息)
     *
     * @param byteBuffer 内存数据
     * @param checkCRC   是否检查CRC
     * @param readBody   是否读取消息体
     * @return 解析结果, {@link DispatchRequest#getMsgSize()} 0 文件末尾, >0 正常消息, -1 消息读取失败
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC, final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MESSAGE_MAGIC_CODE:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            // 消息来源主机地址的解析
            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            // 消息存储broker主机地址的解析
            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            // 读取消息体
            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);
                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            byte topicLen = byteBuffer.get();
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            // 解析消息的额外属性 properties
            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);
                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);

                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // 延迟消息解析(如果该消息是延迟消息的话)
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {

                        // 解析出延迟消息的延迟等级
                        int delayLevel = Integer.parseInt(t);
                        if (delayLevel > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel();
                        }

                        // 通过延迟等级和存储时间, 计算实际要被投递的时间, 存储到tagsCode中
                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.getScheduleMessageService().computeDeliverTimestamp(delayLevel, storeTimestamp);
                        }
                    }
                }
            }

            int readLength = calMsgLength(sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error("[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}", totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            return new DispatchRequest(topic, queueId, physicOffset, totalSize, tagsCode, storeTimestamp, queueOffset, keys, uniqKey, sysFlag, preparedTransactionOffset, propertiesMap);
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    public long getConfirmOffset() {
        return this.confirmOffset;
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    /**
     * 异步的方式添加消息
     *
     * @param msg 单条消息
     * @return 异步添加结果
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // 暂时不知道为啥这边需要设置时间?
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体CRC校验和
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 返回值
        AppendMessageResult result = null;
        // 用来记录一些指标
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        // 延迟消息相关.
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // rocketMQ 提供的延迟消息功能, 当它发现一个消息被配置了延迟属性时, 它会重新将消息的 topic 和 queueId 更换为内部使用的
            // 延迟队列topic再存储. 最后把消息真实的 topic 和 queueId 保存到 MessageExtBrokerInner.propertiesString 属性中.
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }
                // 消息要存储的主题和队列信息
                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // 备份原先消息真实 topic 和 queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                // 将延迟消息队列和主题设置进去, 消息还是存储在commit log中,
                // 只不过后续线程会将其存到延迟队列专用的 consumer queue中
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        long elapsedTimeInLock = 0;

        // rocketMQ 在创建 MappedFile 的时候, 会执行一次系统调用, 将该文件的内存映射写入到物理内存中, 以加快访问.
        // 它其实就是调用了 org.apache.rocketmq.store.util.LibC.mlock() 方法. 因此当文件写满以后, 就没必要
        // 将其映射到物理内存了, 所以这个变量, 就是来引用即将被释放的MappedFile
        MappedFile unlockMappedFile = null;

        // rocketMQ 的消息都是顺序写, 那怎么体现顺序写, 就是依次写入每一个 MappedFile 中
        // 每次消息都是只写入到最后一个 MappedFile 中.
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 获取锁(两种锁), 意味着写入 commit log 的时候是同步写入
        putMessageLock.lock();
        try {
            // 消息存储的时刻
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly global
            msg.setStoreTimestamp(beginLockTimestamp);

            // rocketMQ第一次使用, 那就不存在 MappedFile. 或者, 当前MappedFile已经写满了.
            // 这两种情况会重新再创建一个.
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0);
            }
            // 这种情况就属于创建失败了, 直接返回结果, 同时将成员变量值 beginTimeInLock 为0
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            // mappedFile 获取成功, 直接存储消息, 然后分析结果
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    // 当前mappedFile文件已经写满了, 将它标识一下, 准备释放被锁定的物理内存
                    unlockMappedFile = mappedFile;
                    // 重新创建一个新的 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    // 再执行一次添加消息的操作
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    // 消息过大
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
//                个人觉得下面这段代码没必要添加
//                case UNKNOWN_ERROR:
//                    beginTimeInLock = 0;
//                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }
            // 存储本次消息花费的时间
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        // 如果存在 mappedFile 文件写满, 释放其对内存空间的锁定.
        // 原先是在这里锁住的：org.apache.rocketmq.store.MappedFile.warmMappedFile()
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计数据
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());

        // 提交刷盘和主从同步数据的请求
        CompletableFuture<PutMessageStatus> flushResultFuture = submitFlushRequest(result, msg);
        CompletableFuture<PutMessageStatus> replicaResultFuture = submitReplicaRequest(result, msg);
        // 方法执行到这边, 调用方线程就返回了
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            // 等到 flushOKFuture 和 replicaOKFuture 执行完毕后
            // 如果执行失败了, 重新赋值返回结果.
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} tags: {} client address: {}", msg.getTopic(), msg.getTags(), msg.getBornHostNameString());
                }
            }
            return putMessageResult;
        });
    }

    /**
     * 异步的方式添加批量消息
     *
     * @param messageExtBatch 批量消息
     * @return 异步批量添加结果
     */
    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        // 跟插入单条消息一样, 暂时不知道这里为啥要设置当前时间
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        // 用来记录添加到 commit log 的结果
        AppendMessageResult result;
        // 用来记录一些指标
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 批量消息要求两点格式：⑴非事务消息、⑵非延迟消息
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());
        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        long elapsedTimeInLock = 0;

        // rocketMQ 在创建 MappedFile 的时候, 会执行一次系统调用, 将该文件的内存映射写入到物理内存中, 以加快访问.
        // 它其实就是调用了 org.apache.rocketmq.store.util.LibC.mlock() 方法. 因此当文件写满以后, 就没必要
        // 将其映射到物理内存了, 所以这个变量, 就是来引用即将被释放的MappedFile
        MappedFile unlockMappedFile = null;

        // rocketMQ 的消息都是顺序写, 那怎么体现顺序写, 就是依次写入每一个 MappedFile 中
        // 每次消息都是只写入到最后一个 MappedFile 中.
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 获取编码器, 将多条消息编码到 org.apache.rocketmq.common.message.MessageExtBatch.encodedBuff
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        // 获取锁(两种锁), 意味着写入 commit log 的时候是同步写入
        putMessageLock.lock();
        try {
            // 消息存储的时刻
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            // rocketMQ第一次使用, 那就不存在 MappedFile. 或者, 当前MappedFile已经写满了.
            // 这两种情况会重新再创建一个.
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            // 这种情况就属于创建失败了, 直接返回结果, 同时将成员变量值 beginTimeInLock 为0
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null));
            }

            // mappedFile 获取成功, 直接存储消息, 然后分析结果
            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    // 当前mappedFile文件已经写满了, 将它标识一下, 准备释放被锁定的物理内存
                    unlockMappedFile = mappedFile;
                    // 重新创建一个新的 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result));
                    }
                    // 再执行一次添加消息的操作
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    // 消息过大
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                case UNKNOWN_ERROR:
                default:
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        // 如果存在 mappedFile 文件写满, 释放其对内存空间的锁定.
        // 原先是在这里锁住的：org.apache.rocketmq.store.MappedFile.warmMappedFile()
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计指标
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());

        // 提交刷盘和主从同步数据的请求
        CompletableFuture<PutMessageStatus> flushOKFuture = submitFlushRequest(result, messageExtBatch);
        CompletableFuture<PutMessageStatus> replicaOKFuture = submitReplicaRequest(result, messageExtBatch);
        // 方法执行到这边, 调用方线程就返回了
        return flushOKFuture.thenCombine(replicaOKFuture, (flushStatus, replicaStatus) -> {
            // 等到 flushOKFuture 和 replicaOKFuture 执行完毕后
            // 如果执行失败了, 重新赋值返回结果.
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
                if (replicaStatus == PutMessageStatus.FLUSH_SLAVE_TIMEOUT) {
                    log.error("do sync transfer other node, wait return, but failed, topic: {} client address: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostNameString());
                }
            }
            return putMessageResult;
        });

    }

    /**
     * 同步的方式添加消息
     *
     * @param msg 单条消息
     * @return 添加结果
     */
    public PutMessageResult putMessage(final MessageExtBrokerInner msg) {
        // 暂时不知道为啥这边需要设置时间?
        msg.setStoreTimestamp(System.currentTimeMillis());
        // 设置消息体CRC校验和
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 返回值
        AppendMessageResult result = null;
        // 用来记录一些指标
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        String topic = msg.getTopic();
        int queueId = msg.getQueueId();

        // 延迟消息相关.
        final int tranType = MessageSysFlag.getTransactionValue(msg.getSysFlag());
        if (tranType == MessageSysFlag.TRANSACTION_NOT_TYPE || tranType == MessageSysFlag.TRANSACTION_COMMIT_TYPE) {
            // rocketMQ 提供的延迟消息功能, 当它发现一个消息被配置了延迟属性时, 它会重新将消息的 topic 和 queueId 更换为内部使用的
            // 延迟队列topic再存储. 最后把消息真实的 topic 和 queueId 保存到 MessageExtBrokerInner.propertiesString 属性中.
            if (msg.getDelayTimeLevel() > 0) {
                if (msg.getDelayTimeLevel() > this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel()) {
                    msg.setDelayTimeLevel(this.defaultMessageStore.getScheduleMessageService().getMaxDelayLevel());
                }
                // 消息要存储的主题和队列信息
                topic = TopicValidator.RMQ_SYS_SCHEDULE_TOPIC;
                queueId = ScheduleMessageService.delayLevel2QueueId(msg.getDelayTimeLevel());

                // 备份原先消息真实 topic 和 queueId
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_TOPIC, msg.getTopic());
                MessageAccessor.putProperty(msg, MessageConst.PROPERTY_REAL_QUEUE_ID, String.valueOf(msg.getQueueId()));
                msg.setPropertiesString(MessageDecoder.messageProperties2String(msg.getProperties()));

                // 将延迟消息队列和主题设置进去, 所以此条消息实际保存在延迟队列
                msg.setTopic(topic);
                msg.setQueueId(queueId);
            }
        }

        // IPV6 地址相关
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;

        // rocketMQ 在创建 MappedFile 的时候, 会执行一次系统调用, 将该文件的内存映射写入到物理内存中, 以加快访问.
        // 它其实就是调用了 org.apache.rocketmq.store.util.LibC.mlock() 方法. 因此当文件写满以后, 就没必要
        // 将其映射到物理内存了, 所以这个变量, 就是来引用即将被释放的MappedFile
        MappedFile unlockMappedFile = null;

        // rocketMQ 的消息都是顺序写, 那怎么体现顺序写, 就是依次写入每一个 MappedFile 中
        // 每次消息都是只写入到最后一个 MappedFile 中.
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 获取锁(两种锁), 意味着写入 commit log 的时候是同步写入
        putMessageLock.lock();
        try {
            // 消息存储的时刻
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly global
            msg.setStoreTimestamp(beginLockTimestamp);

            // rocketMQ第一次使用, 那就不存在 MappedFile. 或者, 当前MappedFile已经写满了.
            // 这两种情况会重新再创建一个.
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            // 这种情况就属于创建失败了, 直接返回结果, 同时将成员变量值 beginTimeInLock 为0
            if (null == mappedFile) {
                log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }
            // mappedFile 获取成功, 直接存储消息, 然后分析结果
            result = mappedFile.appendMessage(msg, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    // 当前mappedFile文件已经写满了, 将它标识一下, 准备释放被锁定的物理内存
                    unlockMappedFile = mappedFile;
                    // 重新创建一个新的 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        // XXX: warn and notify me
                        log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    // 再执行一次添加消息的操作
                    result = mappedFile.appendMessage(msg, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    // 消息过大
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }
            // 存储本次消息花费的时间
            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }
        // 如果存在 mappedFile 文件写满, 释放其对内存空间的锁定.
        // 原先是在这里锁住的：org.apache.rocketmq.store.MappedFile.warmMappedFile()
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计数据
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).incrementAndGet();
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).addAndGet(result.getWroteBytes());
        // 处理刷盘
        handleDiskFlush(result, putMessageResult, msg);
        // 处理主从复制
        handleHA(result, putMessageResult, msg);

        return putMessageResult;
    }

    /**
     * 同步的方式添加消息
     *
     * @param messageExtBatch 批量消息
     * @return 添加结果
     */
    public PutMessageResult putMessages(final MessageExtBatch messageExtBatch) {
        // 暂时不知道为啥这边需要设置时间?
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        // 返回值
        AppendMessageResult result;
        // 用来记录一些指标
        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        // 批量消息要求两点格式：⑴非事务消息、⑵非延迟消息
        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());
        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null);
        }

        // IPV6 地址相关
        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }
        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;

        // rocketMQ 在创建 MappedFile 的时候, 会执行一次系统调用, 将该文件的内存映射写入到物理内存中, 以加快访问.
        // 它其实就是调用了 org.apache.rocketmq.store.util.LibC.mlock() 方法. 因此当文件写满以后, 就没必要
        MappedFile unlockMappedFile = null;

        // rocketMQ 的消息都是顺序写, 那怎么体现顺序写, 就是依次写入每一个 MappedFile 中
        // 每次消息都是只写入到最后一个 MappedFile 中.
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        // 获取编码器, 将多条消息编码到 org.apache.rocketmq.common.message.MessageExtBatch.encodedBuff
        MessageExtBatchEncoder batchEncoder = batchEncoderThreadLocal.get();
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch));

        // 获取锁(两种锁), 意味着写入 commit log 的时候是同步写入
        putMessageLock.lock();
        try {
            // 消息存储的时刻
            long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
            this.beginTimeInLock = beginLockTimestamp;

            // Here settings are stored timestamp, in order to ensure an orderly global
            messageExtBatch.setStoreTimestamp(beginLockTimestamp);

            // rocketMQ第一次使用, 那就不存在 MappedFile. 或者, 当前MappedFile已经写满了.
            // 这两种情况会重新再创建一个.
            if (null == mappedFile || mappedFile.isFull()) {
                mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
            }
            // 这种情况就属于创建失败了, 直接返回结果, 同时将成员变量值 beginTimeInLock 为0
            if (null == mappedFile) {
                log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                beginTimeInLock = 0;
                return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, null);
            }
            // mappedFile 获取成功, 直接存储消息, 然后分析结果
            result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
            switch (result.getStatus()) {
                case PUT_OK:
                    break;
                case END_OF_FILE:
                    // 当前mappedFile文件已经写满了, 将它标识一下, 准备释放被锁定的物理内存
                    unlockMappedFile = mappedFile;
                    // 重新创建一个新的 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                    if (null == mappedFile) {
                        log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                        beginTimeInLock = 0;
                        return new PutMessageResult(PutMessageStatus.CREATE_MAPEDFILE_FAILED, result);
                    }
                    // 再执行一次添加消息的操作
                    result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback);
                    break;
                case MESSAGE_SIZE_EXCEEDED:
                case PROPERTIES_SIZE_EXCEEDED:
                    // 消息过大
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result);
                case UNKNOWN_ERROR:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
                default:
                    beginTimeInLock = 0;
                    return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result);
            }

            elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
            beginTimeInLock = 0;
        } finally {
            putMessageLock.unlock();
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        // 如果存在 mappedFile 文件写满, 释放其对内存空间的锁定.
        // 原先是在这里锁住的：org.apache.rocketmq.store.MappedFile.warmMappedFile()
        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // 统计指标
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).addAndGet(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).addAndGet(result.getWroteBytes());
        // 处理刷盘
        handleDiskFlush(result, putMessageResult, messageExtBatch);
        // 处理主从复制
        handleHA(result, putMessageResult, messageExtBatch);

        return putMessageResult;
    }

    /**
     * 提交刷盘请求, 不会等待刷盘结果,
     * 区别于{@link #handleDiskFlush(AppendMessageResult, PutMessageResult, MessageExt)} e}
     *
     * @param result     保存消息后的结果
     * @param messageExt 消息
     * @return 刷盘结果
     */
    public CompletableFuture<PutMessageStatus> submitFlushRequest(AppendMessageResult result, MessageExt messageExt) {
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            if (messageExt.isWaitStoreMsgOK()) {
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                service.putRequest(request);
                return request.future();
            } else {
                service.wakeup();
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }
        // 异步刷盘
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }
    }

    /**
     * 提交副本请求, 不会等待副本执行结果, 区别于
     * {@link #handleHA(AppendMessageResult, PutMessageResult, MessageExt)}
     *
     * @param result     保存消息后的结果
     * @param messageExt 消息
     * @return 更新结果
     */
    public CompletableFuture<PutMessageStatus> submitReplicaRequest(AppendMessageResult result, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // 如果slave可用, master提交副本同步请求
                if (service.isSlaveOK(result.getWroteBytes() + result.getWroteOffset())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    return request.future();
                } else {
                    return CompletableFuture.completedFuture(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }
        return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
    }

    /**
     * 刷盘, 并且等待刷盘结果
     *
     * @param result           消息追加结果
     * @param putMessageResult 要返回给客户端的结果
     * @param messageExt       消息
     */
    public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        // 同步刷盘
        if (FlushDiskType.SYNC_FLUSH == this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
            // rocketMQ 同步刷盘的实现类用的是 GroupCommitService
            final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
            // 如果需要等待消息刷盘的结果
            if (messageExt.isWaitStoreMsgOK()) {
                // 封装刷盘请求
                GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                // 提交请求
                service.putRequest(request);
                // 获取刷盘结果
                CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                PutMessageStatus flushStatus = null;
                try {
                    // 同步等待刷盘结果
                    flushStatus = flushOkFuture.get(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    //flushOK=false;
                }
                if (flushStatus != PutMessageStatus.PUT_OK) {
                    log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
                    putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }
            } else {
                // TODO 这边直接唤醒有效果吗？是不是没有提交刷盘的请求给 GroupCommitRequest
                service.wakeup();
            }
        }
        // 异步刷盘. 直接唤醒后台线程即可
        else {
            if (!this.defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                flushCommitLogService.wakeup();
            } else {
                commitLogService.wakeup();
            }
        }
    }

    /**
     * 提交副本, 并且等待副本执行结果
     *
     * @param result           消息追加结果
     * @param putMessageResult 要返回给客户端的结果
     * @param messageExt       消息
     */
    public void handleHA(AppendMessageResult result, PutMessageResult putMessageResult, MessageExt messageExt) {
        if (BrokerRole.SYNC_MASTER == this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            HAService service = this.defaultMessageStore.getHaService();
            if (messageExt.isWaitStoreMsgOK()) {
                // 如果slave可用, master提交副本同步请求
                if (service.isSlaveOK(result.getWroteOffset() + result.getWroteBytes())) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes());
                    service.putRequest(request);
                    service.getWaitNotifyObject().wakeupAll();
                    PutMessageStatus replicaStatus = null;
                    try {
                        // 同步等待副本执行结果
                        replicaStatus = request.future().get(this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        // ignore
                    }
                    if (replicaStatus != PutMessageStatus.PUT_OK) {
                        log.error("do sync transfer other node, wait return, but failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostNameString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
                    }
                } else {
                    // slave 不可达
                    putMessageResult.setPutMessageStatus(PutMessageStatus.SLAVE_NOT_AVAILABLE);
                }
            }
        }

    }

    /**
     * 根据物理偏移量和消息大小, 查询消息的存储时间
     *
     * @param offset 物理偏移量, 即算入了{@link MappedFile#getFileFromOffset()}
     * @param size   消息大小
     * @return 存储时间戳, 如果发生错误, 返回-1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    /**
     * 根据物理偏移量和消息大小查找消息
     *
     * @param offset 物理偏移量(即算入了{@link MappedFile#getFileFromOffset()})
     * @param size   消息大小
     * @return 实际消息
     */
    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        // 获取 commit log 文件的大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        // 查找指定偏移量位于哪一个mappedFile中
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            // 获取相对偏移量
            int pos = (int) (offset % mappedFileSize);
            // 从mappedFile中捞出数据
            return mappedFile.selectMappedBuffer(pos, size);
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public HashMap<String, Long> getTopicQueueTable() {
        return topicQueueTable;
    }

    public void setTopicQueueTable(HashMap<String, Long> topicQueueTable) {
        this.topicQueueTable = topicQueueTable;
    }

    public boolean appendData(long startOffset, byte[] data) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void removeQueueFromTopicQueueTable(final String topic, final int queueId) {
        String key = topic + "-" + queueId;
        synchronized (this) {
            this.topicQueueTable.remove(key);
        }

        log.info("removeQueueFromTopicQueueTable OK Topic: {} QueueId: {}", topic, queueId);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * 判断一个{@link MappedFile}是否满足用来异常关闭后的恢复条件
     *
     * @param mappedFile 映射文件
     * @return true-满足恢复条件
     */
    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
        // MessageDecoder.MESSAGE_MAGIC_CODE_POSTION 表示消息魔数的索引,
        // 获取这个文件第一条消息的魔数值
        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSTION);
        // 魔数值有误, 那么这个文件就不能作为恢复的数据
        if (magicCode != MESSAGE_MAGIC_CODE) {
            return false;
        }

        // 获取存储时间, 等于0说明数据有误, 这个文件就不能作为恢复的数据
        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable() && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}", storeTimestamp, UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }
        return false;
    }

    /**
     * 封装组提交的请求
     */
    public static class GroupCommitRequest {

        /**
         * 刷盘点
         */
        private final long nextOffset;
        private final long startTimestamp = System.currentTimeMillis();

        /**
         * 刷盘结果
         */
        private CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private long timeoutMillis = Long.MAX_VALUE;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.timeoutMillis = timeoutMillis;
        }

        public GroupCommitRequest(long nextOffset) {
            this.nextOffset = nextOffset;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public void wakeupCustomer(final PutMessageStatus putMessageStatus) {
            this.flushOKFuture.complete(putMessageStatus);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    /**
     * 批量消息编码器
     */
    public static class MessageExtBatchEncoder {
        // Store the message content
        private final ByteBuffer msgBatchMemory;
        // The maximum length of the message
        private final int maxMessageSize;

        MessageExtBatchEncoder(final int size) {
            this.msgBatchMemory = ByteBuffer.allocateDirect(size);
            this.maxMessageSize = size;
        }

        public ByteBuffer encode(final MessageExtBatch messageExtBatch) {
            msgBatchMemory.clear(); //not thread-safe
            int totalMsgLen = 0;
            ByteBuffer messagesByteBuff = messageExtBatch.wrap();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);

            // properties from MessageExtBatch
            String batchPropStr = MessageDecoder.messageProperties2String(messageExtBatch.getProperties());
            final byte[] batchPropData = batchPropStr.getBytes(MessageDecoder.CHARSET_UTF8);
            final short batchPropLen = (short) batchPropData.length;

            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                messagesByteBuff.getInt();
                // 2 MAGICCODE
                messagesByteBuff.getInt();
                // 3 BODYCRC
                messagesByteBuff.getInt();
                // 4 FLAG
                int flag = messagesByteBuff.getInt();
                // 5 BODY
                int bodyLen = messagesByteBuff.getInt();
                int bodyPos = messagesByteBuff.position();
                int bodyCrc = UtilAll.crc32(messagesByteBuff.array(), bodyPos, bodyLen);
                messagesByteBuff.position(bodyPos + bodyLen);
                // 6 properties
                short propertiesLen = messagesByteBuff.getShort();
                int propertiesPos = messagesByteBuff.position();
                messagesByteBuff.position(propertiesPos + propertiesLen);

                final byte[] topicData = messageExtBatch.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);

                final int topicLength = topicData.length;

                final int msgLen = calMsgLength(messageExtBatch.getSysFlag(), bodyLen, topicLength, propertiesLen + batchPropLen);

                // Exceeds the maximum message
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen + ", maxMessageSize: " + this.maxMessageSize);
                    throw new RuntimeException("message size exceeded");
                }

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if (totalMsgLen > maxMessageSize) {
                    throw new RuntimeException("message size exceeded");
                }

                // 1 TOTALSIZE
                this.msgBatchMemory.putInt(msgLen);
                // 2 MAGICCODE
                this.msgBatchMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
                // 3 BODYCRC
                this.msgBatchMemory.putInt(bodyCrc);
                // 4 QUEUEID
                this.msgBatchMemory.putInt(messageExtBatch.getQueueId());
                // 5 FLAG
                this.msgBatchMemory.putInt(flag);
                // 6 QUEUEOFFSET, 默认置为0, 后续在 DefaultAppendMessageCallback.doAppend()方法中重新设置
                this.msgBatchMemory.putLong(0);
                // 7 PHYSICALOFFSET, 置为0, 后续在 DefaultAppendMessageCallback.doAppend()方法中重新设置
                this.msgBatchMemory.putLong(0);
                // 8 SYSFLAG
                this.msgBatchMemory.putInt(messageExtBatch.getSysFlag());
                // 9 BORNTIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getBornTimestamp());
                // 10 BORNHOST
                this.resetByteBuffer(bornHostHolder, bornHostLength);
                this.msgBatchMemory.put(messageExtBatch.getBornHostBytes(bornHostHolder));
                // 11 STORETIMESTAMP
                this.msgBatchMemory.putLong(messageExtBatch.getStoreTimestamp());
                // 12 STOREHOSTADDRESS
                this.resetByteBuffer(storeHostHolder, storeHostLength);
                this.msgBatchMemory.put(messageExtBatch.getStoreHostBytes(storeHostHolder));
                // 13 RECONSUMETIMES
                this.msgBatchMemory.putInt(messageExtBatch.getReconsumeTimes());
                // 14 Prepared Transaction Offset, batch does not support transaction
                this.msgBatchMemory.putLong(0);
                // 15 BODY
                this.msgBatchMemory.putInt(bodyLen);
                if (bodyLen > 0) this.msgBatchMemory.put(messagesByteBuff.array(), bodyPos, bodyLen);
                // 16 TOPIC
                this.msgBatchMemory.put((byte) topicLength);
                this.msgBatchMemory.put(topicData);
                // 17 PROPERTIES
                this.msgBatchMemory.putShort((short) (propertiesLen + batchPropLen));
                if (propertiesLen > 0) {
                    this.msgBatchMemory.put(messagesByteBuff.array(), propertiesPos, propertiesLen);
                }
                if (batchPropLen > 0) {
                    this.msgBatchMemory.put(batchPropData, 0, batchPropLen);
                }
            }
            msgBatchMemory.flip();
            return msgBatchMemory;
        }

        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }

    /**
     * 提交日志和刷盘日志的抽象父类, 它是一个独立线程, 仅仅定义了最大重试次数.
     * rocketMQ 提供三种刷盘策略, 性能由高到低：
     *
     * @see CommitRealTimeService
     * @see FlushRealTimeService
     * @see GroupCommitService
     */
    abstract class FlushCommitLogService extends ServiceThread {
        // 最大重试次数.
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 独立线程, 在间隔时间内重复地执行commit操作. 一旦有数据成功 commit 了,
     * 它会唤醒 {@link #flushCommitLogService} 执行刷盘.
     */
    class CommitRealTimeService extends FlushCommitLogService {

        /**
         * 记录上一次执行 commit 操作的时间戳
         */
        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
                // 获取执行 commit 操作的时间间隔
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();

                // 一次commit至少需要脏页的数量, 0表示不限制
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();

                // Commitlog两次提交的最大间隔, 如果超过该间隔, 将忽略commitCommitLogLeastPages直接提交
                int commitDataThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                // 距离上次执行 commit 操作, 已经超过了 commitDataThoroughInterval 规定的间隔, 因此将
                // commitDataLeastPages 置为0, 表示不再限制至少需要脏页才能commit
                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    // 提交 commit 操作, 最终是交付到 mappedFileQueue → mappedFile 去执行 commit.
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();
                    // 返回false表示有部分数据commit
                    if (!result) {
                        // 此次commit有效, 记录这次提交的时间点
                        this.lastCommitTimestamp = end;
                        // 然后唤醒 flush thread 可以执行刷盘了.
                        flushCommitLogService.wakeup();
                    }

                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    // 阻塞一段时间后, 继续执行.
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 代码可以执行到这边, 说明本线程被停止了
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                // 当线程停止时, 会提交所有数据, 直到 MappedFileQueue#commit() 方法返回true.
                // 那就表示已经没有数据可以 commit 了, 此时for循环就会退出, 表示已经 commit 完成了.
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 独立线程, 在间隔时间内重复执行 flush 刷盘操作. 仅当配置了异步刷盘才生效.
     *
     * @see GroupCommitService-同步刷盘实现
     */
    class FlushRealTimeService extends FlushCommitLogService {
        /**
         * 上一次刷盘的时间点
         */
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {

                // true-表示实时刷盘, false-表示定时刷盘
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();

                // 连续两次刷盘的时间间隔
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();

                // 一次刷盘至少需要的脏页数量
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();

                // commitlog连续两次刷盘的最大间隔, 如果超过该间隔, 将忽略上面 flushCommitLogLeastPages(flushPhysicQueueLeastPages) 的限制
                int flushPhysicQueueThoroughInterval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // 距离上次执行 flush 操作, 已经超过了 flushPhysicQueueThoroughInterval 规定的间隔, 因此将
                // flushPhysicQueueLeastPages 置为0, 表示不再限制至少需要多少脏页才能flush
                long currentTimeMillis = System.currentTimeMillis();
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    // 打印刷盘信息,  每刷10次打印一次
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    if (flushCommitLogTimed) {
                        // 如果是实时刷盘, 那么直接 sleep() 指定的时间间隔
                        Thread.sleep(interval);
                    } else {
                        // 如果是定时刷盘, 那么进入阻塞状态, 释放CPU资源, 等待超时or唤醒.
                        this.waitForRunning(interval);
                    }

                    // 打印刷盘的信息
                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    // 提交 flush 操作, 最终是交付到 mappedFileQueue → mappedFile 去执行 flush.
                    long begin = System.currentTimeMillis();
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);

                    // 获取 mappedFileQueue 的刷盘时间点, 如果大于0, 那么将其记录如到 checkPoint 文件中
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    // 耗时记录
                    long past = System.currentTimeMillis() - begin;
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // 代码可以执行到这边, 说明本线程被停止了, 那么需要保证在退出前执行完 flush 操作.
            boolean result = false;
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                // 当线程停止时, 会刷盘所有数据, 直到 MappedFileQueue#flush() 方法返回true.
                // 那就表示已经没有数据可以 flush 了, 此时for循环就会退出, 表示已经 flush 完成.
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            this.printFlushProgress();
            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 独立线程, 会一直尝试执行 flush 刷盘操作, 仅当配置了同步刷盘才生效.
     * 这个刷盘策略有一个两点：那就是读写分离：它把存储消息的请求和刷盘消息的请求分别用
     * 两个集合存储, 然后每次刷盘时, 将这两个集合互换, 避免了锁的竞争.
     *
     * @see FlushRealTimeService-异步刷盘实现
     */
    class GroupCommitService extends FlushCommitLogService {
        /**
         * 用来保存存储消息的请求
         */
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<GroupCommitRequest>();

        /**
         * 用来读取待刷盘消息的请求
         */
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<GroupCommitRequest>();

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            // 任务线程会一直在这里执行, 直到被关闭.
            while (!this.isStopped()) {
                try {
                    // 在这里阻塞等待10ms, 等待唤醒或者超时返回（甚至如果已经被唤醒过了, 此时就不会阻塞）,
                    // 注意这个方法存在唤醒后的后置处理逻辑, 它会调用 swapRequests() 将两个集合互换.
                    this.waitForRunning(10);
                    // 紧接着就开始执行刷盘操作.
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 代码能走到这里, 说明 rocketMQ 被正常关闭了.
            // Under normal circumstances shutdown, wait for the arrival of the request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            // 获取这个对象锁, 它只会与 putRequest() 方法产生竞争关系.
            synchronized (this) {
                this.swapRequests();
            }
            this.doCommit();
            CommitLog.log.info(this.getServiceName() + " service end");
        }

        /**
         * 添加消息存储的请求, 供外部线程调用.
         *
         * @param request 存储消息的请求
         */
        public synchronized void putRequest(final GroupCommitRequest request) {
            // 锁住写入集合对象, 添加请求
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            // 调用此方法的其它线程, 会唤醒运行 GroupCommitService 的线程.
            // 其实就是下面 run() 方法的 waitForRunning() 方法, 让线程从这里返回.
            this.wakeup();
        }

        /**
         * 将读写集合互换, 供 GroupCommitService 内部线程自己调用.
         * 只有在{@link #onWaitEnd()}和被关闭时被调用
         */
        private void swapRequests() {
            // 每次调用 doCommit() 方法之前, 都会先将读写两集合互换, 这样子就能让
            // putRequest() 和 doCommit() 这两处的 synchronized 同步锁锁住不同的对象.
            // 避免同步刷盘任务与其它生产者提交消息时产生的锁竞争（如果只存在一个集合, 必定只有一个线程能够处理, 另一个线程只能阻塞等待）
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        /**
         * 刷盘
         */
        private void doCommit() {
            synchronized (this.requestsRead) {
                // 这边的 requestsRead, 就是之前调用 putRequest() 添加的请求.
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // 判断当前刷入的位置是否大于等于下一个消息的偏移量, 如果大于那就没必要刷盘了, for循环结束;
                        // 如果小于, rocketMQ选择最多两次刷盘
                        boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        for (int i = 0; i < 2 && !flushOK; i++) {
                            CommitLog.this.mappedFileQueue.flush(0);
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                        }
                        // 如果有数据刷盘成功后, 设置结果：org.apache.rocketmq.store.CommitLog.GroupCommitRequest.flushOKFuture,
                        // 这样子阻塞在 future 的 producer 就知道消息刷盘成功，便可以返回
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    // 设置 checkpoint 的刷盘时间
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    // 清空请求, 以供下次 swapRequests() 后给生产者添加消息使用
                    this.requestsRead.clear();
                } else {
                    // Because of individual messages is set to not sync flush, it will come to this process
                    CommitLog.this.mappedFileQueue.flush(0);
                }
            }
        }

        @Override
        protected void onWaitEnd() {
            // 每次被唤醒后, 就会调用此方法, 将读写集合互换
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJointime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 实际存储消息的逻辑实现
     */
    @SuppressWarnings("Java8MapApi")
    class DefaultAppendMessageCallback implements AppendMessageCallback {
        /**
         * commitlog 文件最少会空闲8个字节：
         * - 高4字节, 用来存储当前文件的剩余空间
         * - 低4字节, 用来存储魔数, 也即{@link #BLANK_MAGIC_CODE}
         */
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;

        /**
         * 当broker主机是 IPV4 时, 使用这个buffer生成消息id
         */
        private final ByteBuffer msgIdMemory;

        /**
         * 当broker主机是 IPV6 时, 使用这个buffer生成消息id
         */
        private final ByteBuffer msgIdV6Memory;

        /**
         * 作为存储消息的临时缓冲区, 消息先会写在这边, 然后再一次性写回到文件映射缓冲区
         */
        private final ByteBuffer msgStoreItemMemory;

        /**
         * 消息最大长度, 超过此长度就会拒绝写入
         */
        private final int maxMessageSize;

        /**
         * 根据 topic + queueId 生成key, 用于在{@link  #topicQueueTable}获取consumer queue偏移量
         */
        private final StringBuilder keyBuilder = new StringBuilder();

        /**
         * 用于生成消息唯一标识, 如果是批量消息, 会把多条消息各自的msgId合到这里
         */
        private final StringBuilder msgIdBuilder = new StringBuilder();

        DefaultAppendMessageCallback(final int size) {
            this.msgIdMemory = ByteBuffer.allocate(4 + 4 + 8);
            this.msgIdV6Memory = ByteBuffer.allocate(16 + 4 + 8);
            this.msgStoreItemMemory = ByteBuffer.allocate(size + END_FILE_MIN_BLANK_LENGTH);
            this.maxMessageSize = size;
        }

        public ByteBuffer getMsgStoreItemMemory() {
            return msgStoreItemMemory;
        }

        /**
         * 追加单条消息的实际逻辑
         *
         * @param fileFromOffset {@link MappedFile}文件的起始地址
         * @param byteBuffer     用来写入数据的缓冲区, 它可能是{@link MappedByteBuffer}那就会写入到pageCache;
         *                       也可能是{@link java.nio.DirectByteBuffer}那就只会写入到物理内存中.
         * @param maxBlank       上面这个缓冲区最大可写的字节数
         * @param msgInner       消息
         * @return 存储结果
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBrokerInner msgInner) {
            // 消息起始偏移量 = 磁盘文件起始偏移量 + 缓冲区当前可写偏移量
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // IPV4地址为4个字节, IPV6地址为16个字节, 加上端口4个字节, 所以就是下面这样子
            int sysflag = msgInner.getSysFlag();
            int bornHostLength = (sysflag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 用来存储IP地址使用
            ByteBuffer bornHostHolder = ByteBuffer.allocate(bornHostLength);
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            this.resetByteBuffer(storeHostHolder, storeHostLength);

            // 生成 message id
            String msgId;
            if ((sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                msgId = MessageDecoder.createMessageId(this.msgIdMemory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            } else {
                msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, msgInner.getStoreHostBytes(storeHostHolder), wroteOffset);
            }

            // 根据 topic + queueId, 生成key, 用来获取 consumer queue 偏移量
            keyBuilder.setLength(0);
            keyBuilder.append(msgInner.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(msgInner.getQueueId());
            String key = keyBuilder.toString();
            // 获取 consumer queue 偏移量, 初始值为0
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            // 事务消息
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // 两种事务消息不会进入到 consumer queue：⑴事务准备消息、⑵事务回滚消息
                // 所以将 queueOffset 置为0
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            /*
             * 开始序列化消息体
             */

            // 消息额外的配置
            final byte[] propertiesData = msgInner.getPropertiesString() == null ? null : msgInner.getPropertiesString().getBytes(MessageDecoder.CHARSET_UTF8);
            final int propertiesLength = propertiesData == null ? 0 : propertiesData.length;
            if (propertiesLength > Short.MAX_VALUE) {
                log.warn("putMessage message properties length too long. length={}", propertiesData.length);
                return new AppendMessageResult(AppendMessageStatus.PROPERTIES_SIZE_EXCEEDED);
            }

            // 消息主题
            final byte[] topicData = msgInner.getTopic().getBytes(MessageDecoder.CHARSET_UTF8);
            final int topicLength = topicData.length;

            // 消息内容
            final int bodyLength = msgInner.getBody() == null ? 0 : msgInner.getBody().length;

            // 最终计算消息存储到磁盘需要的字节数, 这里面会添加各式各样的数值
            final int msgLen = calMsgLength(msgInner.getSysFlag(), bodyLength, topicLength, propertiesLength);

            // 不能超过最大消息大小
            if (msgLen > this.maxMessageSize) {
                CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLength + ", maxMessageSize: " + this.maxMessageSize);
                return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
            }

            // 超过了缓冲区可写的大小, 即已经写到 mappedFile 文件末尾
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                // 重置 msgStoreItemMemory, 将其限制可写临界值为 maxBlank, 也就是 ByteBuffer 的剩余可写字节数量了：
                // 1.写入当前可写的字节数量
                // 2.写入标识mappedFile文件结尾的魔数值
                // 3.剩下空间可以随意写东西
                this.resetByteBuffer(this.msgStoreItemMemory, maxBlank);
                this.msgStoreItemMemory.putInt(maxBlank);
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);

                // 将标识文件末尾的魔数写入到mappedFile中
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, maxBlank);

                // 返回消息追加结果, 已写入到文件末尾, 无法在写入
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgId, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            // 重置 msgStoreItemMemory, 然后准备向里面添加消息
            this.resetByteBuffer(msgStoreItemMemory, msgLen);
            // 1 TOTALSIZE
            this.msgStoreItemMemory.putInt(msgLen);
            // 2 MAGICCODE
            this.msgStoreItemMemory.putInt(CommitLog.MESSAGE_MAGIC_CODE);
            // 3 BODYCRC
            this.msgStoreItemMemory.putInt(msgInner.getBodyCRC());
            // 4 QUEUEID
            this.msgStoreItemMemory.putInt(msgInner.getQueueId());
            // 5 FLAG
            this.msgStoreItemMemory.putInt(msgInner.getFlag());
            // 6 QUEUEOFFSET
            this.msgStoreItemMemory.putLong(queueOffset);
            // 7 PHYSICALOFFSET
            this.msgStoreItemMemory.putLong(fileFromOffset + byteBuffer.position());
            // 8 SYSFLAG
            this.msgStoreItemMemory.putInt(msgInner.getSysFlag());
            // 9 BORNTIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
            // 10 BORNHOST
            this.resetByteBuffer(bornHostHolder, bornHostLength);
            this.msgStoreItemMemory.put(msgInner.getBornHostBytes(bornHostHolder));
            // 11 STORETIMESTAMP
            this.msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
            // 12 STOREHOSTADDRESS
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            this.msgStoreItemMemory.put(msgInner.getStoreHostBytes(storeHostHolder));
            // 13 RECONSUMETIMES
            this.msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
            // 14 Prepared Transaction Offset
            this.msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
            // 15 BODY
            this.msgStoreItemMemory.putInt(bodyLength);
            if (bodyLength > 0) this.msgStoreItemMemory.put(msgInner.getBody());
            // 16 TOPIC
            this.msgStoreItemMemory.put((byte) topicLength);
            this.msgStoreItemMemory.put(topicData);
            // 17 PROPERTIES
            this.msgStoreItemMemory.putShort((short) propertiesLength);
            if (propertiesLength > 0) this.msgStoreItemMemory.put(propertiesData);

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            // 将消息写入队列缓冲区, 此时消息写入到内存或者pageCache中, 还没有落地磁盘
            byteBuffer.put(this.msgStoreItemMemory.array(), 0, msgLen);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgId, msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);

            switch (tranType) {
                // 事务准备和事务回滚消息, 不需要做任何处理
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
                // 非事务和事务提交消息, 需要将对应的 consumer queue 偏移量累加1
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    // The next update ConsumeQueue information
                    CommitLog.this.topicQueueTable.put(key, ++queueOffset);
                    break;
                default:
                    break;
            }

            return result;
        }

        /**
         * 追加批量消息的实际逻辑
         *
         * @param fileFromOffset  {@link MappedFile}文件的起始地址
         * @param byteBuffer      用来写入数据的缓冲区, 它可能是{@link MappedByteBuffer}那就会写入到pageCache;
         *                        也可能是{@link java.nio.DirectByteBuffer}那就只会写入到物理内存中.
         * @param maxBlank        上面这个缓冲区最大可写的字节数
         * @param messageExtBatch 批量消息
         * @return 存储结果
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBatch messageExtBatch) {
            // 标记一下当前缓冲区的position参数, 用于当消息无法写入到当前 mappedFile 时回滚position
            byteBuffer.mark();
            // 消息起始偏移量 = 磁盘文件起始偏移量 + 缓冲区当前可写偏移量
            long wroteOffset = fileFromOffset + byteBuffer.position();

            // Record ConsumeQueue information
            keyBuilder.setLength(0);
            keyBuilder.append(messageExtBatch.getTopic());
            keyBuilder.append('-');
            keyBuilder.append(messageExtBatch.getQueueId());
            String key = keyBuilder.toString();
            Long queueOffset = CommitLog.this.topicQueueTable.get(key);
            if (null == queueOffset) {
                queueOffset = 0L;
                CommitLog.this.topicQueueTable.put(key, queueOffset);
            }

            long beginQueueOffset = queueOffset;

            // 用来记录这一批消息的总大小
            int totalMsgLen = 0;

            // 记录消息的个数
            int msgNum = 0;

            msgIdBuilder.setLength(0);
            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();

            // 这里面是已经经过 MessageExtBatchEncoder 编码好的多条消息的字节数组
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            // 批量消息就不像单条消息存储, 还会统计消息来源方的IP地址了, 这边只处理 broker 的IP地址
            int sysFlag = messageExtBatch.getSysFlag();
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            ByteBuffer storeHostHolder = ByteBuffer.allocate(storeHostLength);
            this.resetByteBuffer(storeHostHolder, storeHostLength);
            ByteBuffer storeHostBytes = messageExtBatch.getStoreHostBytes(storeHostHolder);

            // 记录下 messagesByteBuff 的position属性, 用于当消息无法写入到当前 mappedFile 时回滚position
            messagesByteBuff.mark();

            // 这个循环的作用, 是用来判断一下当前mappedFile是否还有足够空间来存储这一批消息.
            // 同时设置之前在编码批量消息时, 未设置的两个消息属性: QUEUEOFFSET 和 PHYSICALOFFSET
            while (messagesByteBuff.hasRemaining()) {
                // 记录缓冲区的起始读位置
                final int msgPos = messagesByteBuff.position();
                // 获取消息的大小
                final int msgLen = messagesByteBuff.getInt();
                //only for log, just estimate it
                final int bodyLen = msgLen - 40;
                // 超过最大消息限制
                if (msgLen > this.maxMessageSize) {
                    CommitLog.log.warn("message size exceeded, msg total size: " + msgLen + ", msg body size: " + bodyLen + ", maxMessageSize: " + this.maxMessageSize);
                    return new AppendMessageResult(AppendMessageStatus.MESSAGE_SIZE_EXCEEDED);
                }
                // 因为是多条消息, 所以判断当前 mappedFile 是否还有足够空间可以写时, 只能将这些消息的大小累加起来才能判断
                totalMsgLen += msgLen;
                // 如果当前空间不足,
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    // 重置 msgStoreItemMemory, 将其限制可写临界值为 maxBlank, 也就是 ByteBuffer 的剩余可写字节数量了：
                    // 1.写入当前可写的字节数量
                    // 2.写入标识mappedFile文件结尾的魔数值
                    this.resetByteBuffer(this.msgStoreItemMemory, 8);
                    this.msgStoreItemMemory.putInt(maxBlank);
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);

                    // 忽略掉之前循环时, 写入的消息, 重新设置未写入之前的position属性
                    messagesByteBuff.reset();
                    byteBuffer.reset();
                    // 将标识文件末尾的魔数写入到mappedFile中
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    // 返回消息追加结果, 已写入到文件末尾, 无法在写入
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }

                // 之前MessageExtBatchEncoder编码批量消息的时候, 是不会设置 QUEUEOFFSET 和 PHYSICALOFFSET,
                // 因为那时候只有消息数据, 没有 mappedFile 信息, 是没办法设置的, 所以留到这边来设置
                messagesByteBuff.position(msgPos + 20);
                messagesByteBuff.putLong(queueOffset);
                messagesByteBuff.putLong(wroteOffset + totalMsgLen - msgLen);

                // 复用这个storeHostBytes, 存储消息唯一标识
                storeHostBytes.rewind();
                String msgId;
                if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                    msgId = MessageDecoder.createMessageId(this.msgIdMemory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                } else {
                    msgId = MessageDecoder.createMessageId(this.msgIdV6Memory, storeHostBytes, wroteOffset + totalMsgLen - msgLen);
                }

                // 合并这一批消息的各自的唯一标识
                if (msgIdBuilder.length() > 0) {
                    msgIdBuilder.append(',').append(msgId);
                } else {
                    msgIdBuilder.append(msgId);
                }
                // 一条消息对应一个 consumer queue offset, 在处理批量消息时, 每处理一条就累加1
                queueOffset++;
                // 消息个数加1
                msgNum++;
                // 指定下一条消息的起始偏移量, 因为这个while循环里面并没有read操作, 所以position属性不会变
                messagesByteBuff.position(msgPos + msgLen);
            }

            // 将临时缓冲区内的消息, 存储到内存映射缓冲区中, 此时消息还没落地到磁盘
            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);

            // 返回追加结果
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdBuilder.toString(), messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            // 更新 consumer queue 偏移量
            CommitLog.this.topicQueueTable.put(key, queueOffset);

            return result;
        }

        /**
         * 将 position 置为0, 然后设置limit
         */
        private void resetByteBuffer(final ByteBuffer byteBuffer, final int limit) {
            byteBuffer.flip();
            byteBuffer.limit(limit);
        }

    }
}
