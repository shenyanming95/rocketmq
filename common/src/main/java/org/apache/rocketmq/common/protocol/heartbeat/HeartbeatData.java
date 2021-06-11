/**
 * $Id: HeartbeatData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

public class HeartbeatData extends RemotingSerializable {
    private String clientID;
    private Set<ProducerData> producerDataSet = new HashSet<ProducerData>();
    private Set<ConsumerData> consumerDataSet = new HashSet<ConsumerData>();

    public String getClientID() {
        return clientID;
    }

    public void setClientID(String clientID) {
        this.clientID = clientID;
    }

    public Set<ProducerData> getProducerDataSet() {
        return producerDataSet;
    }

    public void setProducerDataSet(Set<ProducerData> producerDataSet) {
        this.producerDataSet = producerDataSet;
    }

    public Set<ConsumerData> getConsumerDataSet() {
        return consumerDataSet;
    }

    public void setConsumerDataSet(Set<ConsumerData> consumerDataSet) {
        this.consumerDataSet = consumerDataSet;
    }

    @Override
    public String toString() {
        return "HeartbeatData [clientID=" + clientID + ", producerDataSet=" + producerDataSet
                + ", consumerDataSet=" + consumerDataSet + "]";
    }
}
