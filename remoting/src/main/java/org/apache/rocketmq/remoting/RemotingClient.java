package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * CS通信结构, 作为Client端的接口抽象.
 *
 * @see RemotingServer
 */
public interface RemotingClient extends RemotingService {

    /**
     * 更新命名服务中心(NameServer)地址
     *
     * @param addrs 地址
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取命名服务中心(NameServer)地址
     *
     * @return 地址集合
     */
    List<String> getNameServerAddressList();

    /**
     * 向Server发出请求(同步)
     *
     * @param addr          server端的地址
     * @param request       请求
     * @param timeoutMillis 超时时间
     * @return server端返回的响应
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 向Server发出请求(异步)
     *
     * @param addr           server端的地址
     * @param request        请求
     * @param timeoutMillis  超时时间
     * @param invokeCallback server响应后的回调逻辑
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 向Server发出请求, 并且不需要响应
     *
     * @param addr          server端的地址
     * @param request       请求
     * @param timeoutMillis 超时时间
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册处理器, 许多开源框架, 都喜欢用Processor封装命令的处理逻辑, 例如Tomcat.
     *
     * @param requestCode 请求码, 用于标识请求类型
     * @param processor   处理器
     * @param executor    线程池
     */
    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    /**
     * 获取异步线程池
     */
    ExecutorService getCallbackExecutor();

    /**
     * 设置异步线程
     */
    void setCallbackExecutor(final ExecutorService callbackExecutor);

    /**
     * 判断与Server的底层通道是否可以写
     *
     * @param addr Server端地址
     * @return true-可写
     */
    boolean isChannelWritable(final String addr);
}
