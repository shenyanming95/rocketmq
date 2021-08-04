package org.apache.rocketmq.store;

/**
 * When write a message to the commit log, returns code
 */
public enum AppendMessageStatus {
    /**
     * 存储消息成功
     */
    PUT_OK,

    /**
     * 超过{@link java.nio.ByteBuffer}可写区域
     */
    END_OF_FILE,

    /**
     * 消息太长
     */
    MESSAGE_SIZE_EXCEEDED,

    /**
     * 消息额外属性太长, {@link org.apache.rocketmq.common.message.Message#properties}
     */
    PROPERTIES_SIZE_EXCEEDED,

    /**
     * 未知异常
     */
    UNKNOWN_ERROR,
}
