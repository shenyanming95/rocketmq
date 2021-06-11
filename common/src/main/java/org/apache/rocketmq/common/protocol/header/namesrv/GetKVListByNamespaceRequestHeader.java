package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetKVListByNamespaceRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String namespace;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
}
