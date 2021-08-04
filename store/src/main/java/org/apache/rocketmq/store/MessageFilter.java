package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 当使用消息存储引擎{@link MessageStore}查询到消息时, 使用这个接口来过滤查询到消息
 */
public interface MessageFilter {
    /**
     * match by tags code or filter bit map which is calculated when message received
     * and stored in consume queue ext.
     *
     * @param tagsCode  tagsCode
     * @param cqExtUnit extend unit of consume queue
     */
    boolean isMatchedByConsumeQueue(final Long tagsCode, final ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * match by message content which are stored in commit log.
     * <br>{@code msgBuffer} and {@code properties} are not all null.If invoked in store,
     * {@code properties} is null;If invoked in {@code PullRequestHoldService}, {@code msgBuffer} is null.
     *
     * @param msgBuffer  message buffer in commit log, may be null if not invoked in store.
     * @param properties message properties, should decode from buffer if null by yourself.
     */
    boolean isMatchedByCommitLog(final ByteBuffer msgBuffer, final Map<String, String> properties);
}
