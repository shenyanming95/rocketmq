package org.apache.rocketmq.store;

/**
 * 存储消息的状态
 */
public enum PutMessageStatus {
    /**
     * 存储消息成功
     */
    PUT_OK,

    /**
     * 刷盘超时
     */
    FLUSH_DISK_TIMEOUT,

    /**
     * 同步从机数据超时
     */
    FLUSH_SLAVE_TIMEOUT,

    /**
     * 从机不可达
     */
    SLAVE_NOT_AVAILABLE,

    /**
     * 服务不可用
     */
    SERVICE_NOT_AVAILABLE,

    /**
     * 创建{@link MappedFile}文件失败
     */
    CREATE_MAPEDFILE_FAILED,

    /**
     * 非法的消息
     */
    MESSAGE_ILLEGAL,

    /**
     * 超过规定的最大大小
     */
    PROPERTIES_SIZE_EXCEEDED,

    /**
     * OS pageCache 繁忙
     */
    OS_PAGECACHE_BUSY,

    /**
     * 未知异常
     */
    UNKNOWN_ERROR,
}
