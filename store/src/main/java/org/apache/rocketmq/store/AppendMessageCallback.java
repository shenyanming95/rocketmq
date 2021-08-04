package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExtBatch;

import java.nio.ByteBuffer;

/**
 * 实际写入消息的接口
 */
public interface AppendMessageCallback {

    /**
     * 写入消息
     *
     * @param fileFromOffset 磁盘文件的开始偏移量
     * @param byteBuffer     消息暂时写入的缓冲区
     * @param maxBlank       最大可写入的字节数
     * @param msg            消息
     * @return 写入了多少字节
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBrokerInner msg);

    /**
     * 写入消息
     *
     * @param fileFromOffset  磁盘文件的开始偏移量
     * @param byteBuffer      消息可以写入的缓冲区
     * @param maxBlank        最大可写消息大小
     * @param messageExtBatch 批量消息
     * @return 写入了多少字节
     */
    AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank, final MessageExtBatch messageExtBatch);
}
