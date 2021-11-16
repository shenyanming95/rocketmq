package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 异步的请求命令处理接口.
 * 其实就是在命令处理方法参数上添加一个回调逻辑, 在请求处理完以后执行这个逻辑.
 */
public abstract class AsyncNettyRequestProcessor implements NettyRequestProcessor {

    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        RemotingCommand response = processRequest(ctx, request);
        responseCallback.callback(response);
    }
}
