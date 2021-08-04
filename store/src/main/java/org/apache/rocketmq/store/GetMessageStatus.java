package org.apache.rocketmq.store;

public enum GetMessageStatus {

    /**
     * 准确找到消息
     */
    FOUND,

    /**
     * 无符合条件的消息
     */
    NO_MATCHED_MESSAGE,

    /**
     * 符合条件的消息已经被移除
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 不存在查询的偏移量
     */
    OFFSET_FOUND_NULL,

    /**
     * 查询的偏移量, 超过消费队列位置太多(大于一个位置)
     *
     * @see {{@link #OFFSET_OVERFLOW_ONE}}
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 查询的偏移量, 超过消费队列一个位置.
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 查询的偏移量过小, 即小于 consumer queue 的最小偏移量
     */
    OFFSET_TOO_SMALL,

    /**
     * 不存在消费队列
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * 消费队列不存在消息, 即 offset=0
     */
    NO_MESSAGE_IN_QUEUE,
}
