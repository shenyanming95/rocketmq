package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class QueryDataVersionResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private Boolean changed;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public Boolean getChanged() {
        return changed;
    }

    public void setChanged(Boolean changed) {
        this.changed = changed;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QueryDataVersionResponseHeader{");
        sb.append("changed=").append(changed);
        sb.append('}');
        return sb.toString();
    }
}
