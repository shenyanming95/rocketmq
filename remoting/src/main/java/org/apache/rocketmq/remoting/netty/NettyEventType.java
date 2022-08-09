package org.apache.rocketmq.remoting.netty;

/**
 * netty事件类型
 */
public enum NettyEventType {
    /**
     * 通道channel连接事件
     */
    CONNECT,

    /**
     * 通道channel关闭事件
     */
    CLOSE,

    /**
     * 通道channel心跳事件
     */
    IDLE,

    /**
     * 读取通道channel数据异常事件
     */
    EXCEPTION
}
