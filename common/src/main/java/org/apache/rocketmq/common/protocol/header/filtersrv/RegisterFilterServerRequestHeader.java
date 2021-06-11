package org.apache.rocketmq.common.protocol.header.filtersrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class RegisterFilterServerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String filterServerAddr;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getFilterServerAddr() {
        return filterServerAddr;
    }

    public void setFilterServerAddr(String filterServerAddr) {
        this.filterServerAddr = filterServerAddr;
    }
}
