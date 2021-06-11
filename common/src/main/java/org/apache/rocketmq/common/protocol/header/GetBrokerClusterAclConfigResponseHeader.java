package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

import java.util.List;

public class GetBrokerClusterAclConfigResponseHeader implements CommandCustomHeader {

    @CFNotNull
    private List<PlainAccessConfig> plainAccessConfigs;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public List<PlainAccessConfig> getPlainAccessConfigs() {
        return plainAccessConfigs;
    }

    public void setPlainAccessConfigs(List<PlainAccessConfig> plainAccessConfigs) {
        this.plainAccessConfigs = plainAccessConfigs;
    }
}
