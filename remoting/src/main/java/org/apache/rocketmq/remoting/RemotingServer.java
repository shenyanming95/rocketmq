package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;
import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.ExecutorService;

/**
 * CS通信结构, 作为Server端的接口抽象.
 * @see RemotingClient
 */
public interface RemotingServer extends RemotingService {

    /**
     * 注册处理器, 许多开源框架, 都喜欢用Processor封装命令的处理逻辑, 例如Tomcat.
     *
     * @param requestCode 请求码, 用于标识请求类型
     * @param processor   处理器
     * @param executor    线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 注册默认的处理器
     *
     * @param processor 处理器
     * @param executor  线程池
     */
    void registerDefaultProcessor(final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 由于是Server端的接口抽象, 作为Server, 肯定有监听的端口.
     *
     * @return 监听的端口号
     */
    int localListenPort();

    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(final int requestCode);

    /**
     * 同步执行client发出的请求
     *
     * @param channel       底层通道
     * @param request       client端发出的请求
     * @param timeoutMillis 处理超时时间
     * @return 发回给client的响应
     */
    RemotingCommand invokeSync(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步执行client发出的请求
     *
     * @param channel        底层通道
     * @param request        client端发出的请求
     * @param timeoutMillis  处理超时时间
     * @param invokeCallback 处理请求后的回调逻辑(有了它才可以实现异步)
     */
    void invokeAsync(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 执行client发出的请求, 且不用给client响应
     *
     * @param channel       底层通道
     * @param request       client端发出的请求
     * @param timeoutMillis 处理超时时间
     */
    void invokeOneway(final Channel channel, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

}
