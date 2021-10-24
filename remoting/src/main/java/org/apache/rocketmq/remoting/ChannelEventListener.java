package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

/**
 * 通道{@link io.netty.channel.Channel}的事件监听器
 */
public interface ChannelEventListener {

    /**
     * 通道连接
     */
    void onChannelConnect(final String remoteAddr, final Channel channel);

    /**
     * 通道关闭
     */
    void onChannelClose(final String remoteAddr, final Channel channel);

    /**
     * 通道异常
     */
    void onChannelException(final String remoteAddr, final Channel channel);

    /**
     * 通道心跳
     */
    void onChannelIdle(final String remoteAddr, final Channel channel);
}
