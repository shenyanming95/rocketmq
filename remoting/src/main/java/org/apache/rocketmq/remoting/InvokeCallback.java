package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 用于：
 * {@link RemotingServer#invokeAsync(Channel, RemotingCommand, long, InvokeCallback)}
 * {@link RemotingClient#invokeAsync(String, RemotingCommand, long, InvokeCallback)}
 * 当请求完成以后, 执行相应的逻辑
 */
public interface InvokeCallback {

    /**
     * 请求or响应完成后, 执行相应的逻辑
     *
     * @param responseFuture 响应信息
     */
    void operationComplete(final ResponseFuture responseFuture);
}
