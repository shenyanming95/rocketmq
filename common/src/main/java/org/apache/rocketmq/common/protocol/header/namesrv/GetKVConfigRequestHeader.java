package org.apache.rocketmq.common.protocol.header.namesrv;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetKVConfigRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String namespace;
    @CFNotNull
    private String key;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
