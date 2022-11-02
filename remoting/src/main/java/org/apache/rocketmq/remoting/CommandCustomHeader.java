package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 自定义命令的消息头接口定义
 */
public interface CommandCustomHeader {
    void checkFields() throws RemotingCommandException;
}
