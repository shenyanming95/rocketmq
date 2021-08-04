package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * 核心接口: rocketMQ 对外暴露的消息存储引擎.
 * 可以使用它的默认实现{@link DefaultMessageStore}, 也可以自定义实现.
 */
public interface MessageStore {

    /**
     * 加载之前存储的消息
     *
     * @return true if success; false otherwise.
     */
    boolean load();

    /**
     * 启动此消息存储引擎
     *
     * @throws Exception if there is any error.
     */
    void start() throws Exception;

    /**
     * 关闭此消息存储引擎
     */
    void shutdown();

    /**
     * 销毁此消息存储引擎, 一般调用后要删除所有已经持久化的文件
     */
    void destroy();

    /**
     * 异步存储单条消息
     *
     * @param msg MessageInstance to store
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        return CompletableFuture.completedFuture(putMessage(msg));
    }

    /**
     * 异步存储批量消息
     *
     * @param messageExtBatch the message batch
     * @return a CompletableFuture for the result of store operation
     */
    default CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        return CompletableFuture.completedFuture(putMessages(messageExtBatch));
    }

    /**
     * 同步存储单条消息
     *
     * @param msg Message instance to store
     * @return result of store operation.
     */
    PutMessageResult putMessage(final MessageExtBrokerInner msg);

    /**
     * 同步存储批量消息
     *
     * @param messageExtBatch Message batch.
     * @return result of storing batch messages.
     */
    PutMessageResult putMessages(final MessageExtBatch messageExtBatch);

    /**
     * 在指定主题(topic)的指定队列(queueId)中, 从指定的偏移量(offset)开始, 最多查询指定数量(maxMsgNums)的消息,
     * 最后使用消息过滤器{@link MessageFilter}进一步筛选结果消息.
     *
     * @param group         发起此次查询的消费组
     * @param topic         消息主题
     * @param queueId       消息队列id
     * @param offset        起始逻辑偏移量
     * @param maxMsgNums    要查询的最大消息量
     * @param messageFilter 用于筛选所需消息的消息过滤器。
     * @return 匹配的消息
     */
    GetMessageResult getMessage(final String group, final String topic, final int queueId,
                                final long offset, final int maxMsgNums, final MessageFilter messageFilter);

    /**
     * 获取指定主题下, 指定队列号的最大偏移量
     *
     * @param topic   主题
     * @param queueId 队列号
     * @return 当前最大偏移量
     */
    long getMaxOffsetInQueue(final String topic, final int queueId);

    /**
     * 获取指定主题下, 指定队列号的最小偏移量
     *
     * @param topic   主题
     * @param queueId 队列号
     * @return 当前最小偏移量
     */
    long getMinOffsetInQueue(final String topic, final int queueId);

    /**
     * 获取在 commit log 中消息的偏移量, 也称为物理偏移量
     *
     * @param topic              主题
     * @param queueId            队列号
     * @param consumeQueueOffset consumer queue 偏移量
     * @return 物理偏移量
     */
    long getCommitLogOffsetInQueue(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 查找存储时间戳位指定时间的消息的物理偏移量
     *
     * @param topic     消息主题
     * @param queueId   队列号
     * @param timestamp 指定的时间戳
     * @return 匹配的物理偏移量
     */
    long getOffsetInQueueByTime(final String topic, final int queueId, final long timestamp);

    /**
     * 通过给定的 commit log 偏移量查找消息
     *
     * @param commitLogOffset 物理偏移量
     * @return 指定物理偏移量的消息
     */
    MessageExt lookMessageByOffset(final long commitLogOffset);

    /**
     * 从指定的 commit log 偏移量中获取一条消息
     *
     * @param commitLogOffset commit log offset.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset);

    /**
     * 从指定的 commit log 偏移量中获取一条消息
     *
     * @param commitLogOffset commit log offset.
     * @param msgSize         message size.
     * @return wrapped result of the message.
     */
    SelectMappedBufferResult selectOneMessageByOffset(final long commitLogOffset, final int msgSize);

    /**
     * 获取此消息存储引擎的运行信息
     *
     * @return message store running info.
     */
    String getRunningDataInfo();

    /**
     * 消息存储运行时信息, 一般应该包含各种统计信息
     *
     * @return runtime information of the message store in format of key-value pairs.
     */
    HashMap<String, String> getRuntimeInfo();

    /**
     * 获取最大 commit log 偏移量
     *
     * @return maximum commit log offset.
     */
    long getMaxPhyOffset();

    /**
     * 获取最小 commit log 偏移量
     *
     * @return minimum commit log offset.
     */
    long getMinPhyOffset();

    /**
     * 获取指定主题 + 指定队列号下, 最早存储的消息的存储时间.
     *
     * @param topic   Topic of the messages to query.
     * @param queueId Queue ID to find.
     * @return store time of the earliest message.
     */
    long getEarliestMessageTime(final String topic, final int queueId);

    /**
     * 获取此消息存储引擎最早存储的消息的存储时间
     *
     * @return timestamp of the earliest message in this store.
     */
    long getEarliestMessageTime();

    /**
     * 获取指定消息的存储时间
     *
     * @param topic              message topic.
     * @param queueId            queue ID.
     * @param consumeQueueOffset consume queue offset.
     * @return store timestamp of the message.
     */
    long getMessageStoreTimeStamp(final String topic, final int queueId, final long consumeQueueOffset);

    /**
     * 获取指定队列中的消息总数
     *
     * @param topic   Topic
     * @param queueId Queue ID.
     * @return total number.
     */
    long getMessageTotalInQueue(final String topic, final int queueId);

    /**
     * 获取从给定偏移量开始的 commit log 数据, 一般用于复制.
     *
     * @param offset starting offset.
     * @return commit log data.
     */
    SelectMappedBufferResult getCommitLogData(final long offset);

    /**
     * 添加数据到 commit log 中
     *
     * @param startOffset starting offset.
     * @param data        data to append.
     * @return true if success; false otherwise.
     */
    boolean appendToCommitLog(final long startOffset, final byte[] data);

    /**
     * 手动执行文件删除.
     */
    void executeDeleteFilesManually();

    /**
     * 按给定键查询消息.
     *
     * @param topic  topic of the message.
     * @param key    message key.
     * @param maxNum maximum number of the messages possible.
     * @param begin  begin timestamp.
     * @param end    end timestamp.
     */
    QueryMessageResult queryMessage(final String topic, final String key, final int maxNum, final long begin, final long end);

    /**
     * Update HA master address.
     *
     * @param newAddr new address.
     */
    void updateHaMasterAddress(final String newAddr);

    /**
     * 返回 slave 未同步 master 消息的字节数
     *
     * @return number of bytes that slave falls behind.
     */
    long slaveFallBehindMuch();

    /**
     * 返回当前存储引擎的时间
     *
     * @return current time in milliseconds since 1970-01-01.
     */
    long now();

    /**
     * 清理未使用的主题
     *
     * @param topics all valid topics.
     * @return number of the topics deleted.
     */
    int cleanUnusedTopic(final Set<String> topics);

    /**
     * 清理过期的 consume queues
     */
    void cleanExpiredConsumerQueue();

    /**
     * 检查给定的消息是否已被置换出内存
     *
     * @param topic         topic.
     * @param queueId       queue ID.
     * @param consumeOffset consume queue offset.
     * @return true if the message is no longer in memory; false otherwise.
     */
    boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset);

    /**
     * 获取已存储在 commit log 中但尚未分派到 consumer queue 的字节数
     *
     * @return number of the bytes to dispatch.
     */
    long dispatchBehindBytes();

    /**
     * 刷新消息存储以保留所有数据
     *
     * @return maximum offset flushed to persistent storage device.
     */
    long flush();

    /**
     * 重置写入偏移量
     *
     * @param phyOffset new offset.
     * @return true if success; false otherwise.
     */
    boolean resetWriteOffset(long phyOffset);

    /**
     * 获取确认偏移量
     *
     * @return confirm offset.
     */
    long getConfirmOffset();

    /**
     * 设置确认偏移量
     *
     * @param phyOffset confirm offset to set.
     */
    void setConfirmOffset(long phyOffset);

    /**
     * 检查 OS page cache 是否繁忙
     *
     * @return true if the OS page cache is busy; false otherwise.
     */
    boolean isOSPageCacheBusy();

    /**
     * 以毫秒为单位获取此消息存储引擎的锁定时间
     *
     * @return lock time in milliseconds.
     */
    long lockTimeMills();

    /**
     * 检查 {@link TransientStorePool} 是否不足
     *
     * @return true if the transient store pool is running out; false otherwise.
     */
    boolean isTransientStorePoolDeficient();

    /**
     * Get the dispatcher list.
     *
     * @return list of the dispatcher.
     */
    LinkedList<CommitLogDispatcher> getDispatcherList();

    /**
     * 获取指定主题, 指定队列号的 consumer queue
     *
     * @param topic   Topic.
     * @param queueId Queue ID.
     * @return Consume queue.
     */
    ConsumeQueue getConsumeQueue(String topic, int queueId);

    /**
     * 获取此消息存储引擎的{@link BrokerStatsManager}
     *
     * @return BrokerStatsManager.
     */
    BrokerStatsManager getBrokerStatsManager();

    /**
     * handle
     *
     * @param brokerRole
     */
    void handleScheduleMessageService(BrokerRole brokerRole);
}
