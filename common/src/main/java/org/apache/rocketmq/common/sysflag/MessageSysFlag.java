package org.apache.rocketmq.common.sysflag;

public class MessageSysFlag {

    /**
     * 压缩标志, 值为1
     */
    public final static int COMPRESSED_FLAG = 0x1;

    /**
     * 多标签标志, 值为2
     */
    public final static int MULTI_TAGS_FLAG = 0x1 << 1;

    /**
     * 事务类型-非事务, 值为0
     */
    public final static int TRANSACTION_NOT_TYPE = 0;

    /**
     * 事务类型-事务准备, 值为4
     */
    public final static int TRANSACTION_PREPARED_TYPE = 0x1 << 2;

    /**
     * 事务类型-提交, 值为8
     */
    public final static int TRANSACTION_COMMIT_TYPE = 0x2 << 2;

    /**
     * 事务类型-回滚, 值为12
     */
    public final static int TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;

    /**
     * 消息来源的 producer 的主机地址, IPV6标志, 值为16
     */
    public final static int BORNHOST_V6_FLAG = 0x1 << 4;

    /**
     * 消息存储的 broker 的主机地址, IPV6标志, 值为32
     */
    public final static int STOREHOSTADDRESS_V6_FLAG = 0x1 << 5; // 32

    public static int getTransactionValue(final int flag) {
        return flag & TRANSACTION_ROLLBACK_TYPE;
    }

    public static int resetTransactionValue(final int flag, final int type) {
        return (flag & (~TRANSACTION_ROLLBACK_TYPE)) | type;
    }

    public static int clearCompressedFlag(final int flag) {
        return flag & (~COMPRESSED_FLAG);
    }

}
