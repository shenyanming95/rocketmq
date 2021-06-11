/**
 * $Id: GetBrokerConfigResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetBrokerConfigResponseHeader implements CommandCustomHeader {
    @CFNotNull
    private String version;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
