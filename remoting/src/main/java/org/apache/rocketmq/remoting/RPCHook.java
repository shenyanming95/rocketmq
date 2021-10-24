package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * RPC调用的钩子函数
 */
public interface RPCHook {

    /**
     * 处理请求之前调用
     *
     * @param remoteAddr 对端地址
     * @param request    请求类型
     */
    void doBeforeRequest(final String remoteAddr, final RemotingCommand request);

    /**
     * 处理请求之后调用
     *
     * @param remoteAddr 对端地址
     * @param request    请求类型
     * @param response   响应值
     */
    void doAfterResponse(final String remoteAddr, final RemotingCommand request, final RemotingCommand response);
}
