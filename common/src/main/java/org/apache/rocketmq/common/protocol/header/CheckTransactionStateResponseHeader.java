/**
 * $Id: EndTransactionResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class CheckTransactionStateResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String producerGroup;
    @CFNotNull
    private Long tranStateTableOffset;
    @CFNotNull
    private Long commitLogOffset;
    @CFNotNull
    private Integer commitOrRollback; // TRANSACTION_COMMIT_TYPE

    // TRANSACTION_ROLLBACK_TYPE

    @Override
    public void checkFields() throws RemotingCommandException {
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
}
