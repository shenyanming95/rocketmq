package org.apache.rocketmq.remoting.protocol;

/**
 * RocketMQ网络请求的响应码
 */
public class RemotingSysResponseCode {

    /**
     * 成功
     */
    public static final int SUCCESS = 0;

    /**
     * 服务端异常
     */
    public static final int SYSTEM_ERROR = 1;

    /**
     * 服务端繁忙
     */
    public static final int SYSTEM_BUSY = 2;

    /**
     * 服务端不支持的请求
     */
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;

    public static final int TRANSACTION_FAILED = 4;
}
