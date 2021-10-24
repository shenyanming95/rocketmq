package org.apache.rocketmq.remoting.protocol;

public enum RemotingCommandType {

    /**
     * 表示这是一个请求的命令
     */
    REQUEST_COMMAND,

    /**
     * 表示这是一个响应的命令
     */
    RESPONSE_COMMAND;
}
