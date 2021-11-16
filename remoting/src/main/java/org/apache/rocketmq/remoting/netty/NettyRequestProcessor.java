package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 通用的命令处理器, 是一种很好的设计思想.
 * 将每一种请求都封装成一个类, 接口定义通用的请求处理方法, 由每个接口实现类去完成自己的命名处理逻辑
 */
public interface NettyRequestProcessor {

    /**
     * 处理请求
     *
     * @param ctx     netty的pipeline上下文
     * @param request 封装请求的命令
     * @return 请求处理结果
     * @throws Exception 请求处理过程中发生的异常
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    /**
     * 判断是否需要拒绝请求
     *
     * @return true-拒绝处理请求, false-允许请假
     */
    boolean rejectRequest();

}
