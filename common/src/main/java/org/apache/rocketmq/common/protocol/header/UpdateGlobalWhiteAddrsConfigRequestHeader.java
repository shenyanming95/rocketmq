package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class UpdateGlobalWhiteAddrsConfigRequestHeader implements CommandCustomHeader {

    @CFNotNull
    private String globalWhiteAddrs;

    @Override
    public void checkFields() throws RemotingCommandException {

    }

    public String getGlobalWhiteAddrs() {
        return globalWhiteAddrs;
    }

    public void setGlobalWhiteAddrs(String globalWhiteAddrs) {
        this.globalWhiteAddrs = globalWhiteAddrs;
    }
}
