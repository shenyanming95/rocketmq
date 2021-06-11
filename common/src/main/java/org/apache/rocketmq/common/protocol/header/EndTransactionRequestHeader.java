package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.annotation.CFNullable;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class EndTransactionRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    private Long tranStateTableOffset;
    @CFNotNull
    private Long commitLogOffset;
    @CFNotNull
    private Integer commitOrRollback; // TRANSACTION_COMMIT_TYPE
    // TRANSACTION_ROLLBACK_TYPE
    // TRANSACTION_NOT_TYPE

    @CFNullable
    private Boolean fromTransactionCheck = false;

    @CFNotNull
    private String msgId;

    private String transactionId;

    @Override
    public void checkFields() throws RemotingCommandException {
        if (MessageSysFlag.TRANSACTION_NOT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_COMMIT_TYPE == this.commitOrRollback) {
            return;
        }

        if (MessageSysFlag.TRANSACTION_ROLLBACK_TYPE == this.commitOrRollback) {
            return;
        }

        throw new RemotingCommandException("commitOrRollback field wrong");
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Long getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    public void setTranStateTableOffset(Long tranStateTableOffset) {
        this.tranStateTableOffset = tranStateTableOffset;
    }

    public Long getCommitLogOffset() {
        return commitLogOffset;
    }

    public void setCommitLogOffset(Long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }

    public Integer getCommitOrRollback() {
        return commitOrRollback;
    }

    public void setCommitOrRollback(Integer commitOrRollback) {
        this.commitOrRollback = commitOrRollback;
    }

    public Boolean getFromTransactionCheck() {
        return fromTransactionCheck;
    }

    public void setFromTransactionCheck(Boolean fromTransactionCheck) {
        this.fromTransactionCheck = fromTransactionCheck;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    @Override
    public String toString() {
        return "EndTransactionRequestHeader{" +
                "producerGroup='" + producerGroup + '\'' +
                ", tranStateTableOffset=" + tranStateTableOffset +
                ", commitLogOffset=" + commitLogOffset +
                ", commitOrRollback=" + commitOrRollback +
                ", fromTransactionCheck=" + fromTransactionCheck +
                ", msgId='" + msgId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                '}';
    }
}
