/**
 * $Id: QueryMessageResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryMessageResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Long indexLastUpdateTimestamp;
    @CFNotNull
    private Long indexLastUpdatePhyoffset;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public Long getIndexLastUpdateTimestamp() {
        return indexLastUpdateTimestamp;
    }

    public void setIndexLastUpdateTimestamp(Long indexLastUpdateTimestamp) {
        this.indexLastUpdateTimestamp = indexLastUpdateTimestamp;
    }

    public Long getIndexLastUpdatePhyoffset() {
        return indexLastUpdatePhyoffset;
    }

    public void setIndexLastUpdatePhyoffset(Long indexLastUpdatePhyoffset) {
        this.indexLastUpdatePhyoffset = indexLastUpdatePhyoffset;
    }
}
