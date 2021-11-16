package org.apache.rocketmq.remoting.netty;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 执行完请求命令后的回调接口, 用于{@link AsyncNettyRequestProcessor}
 */
public interface RemotingResponseCallback {
    void callback(RemotingCommand response);
}
