package org.apache.rocketmq.remoting;

/**
 * 服务接口抽象, 通俗地讲, client是一个RemotingService, server也是一个RemotingService.
 */
public interface RemotingService {

    /**
     * 启动
     */
    void start();

    /**
     * 终止
     */
    void shutdown();

    /**
     * 注册钩子函数
     */
    void registerRPCHook(RPCHook rpcHook);
}
