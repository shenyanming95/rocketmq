/**
 * $Id: GetAllTopicConfigResponseHeader.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.header;

import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public class GetAllTopicConfigResponseHeader implements CommandCustomHeader {

    @Override
    public void checkFields() throws RemotingCommandException {
    }
}
