/**
 * $Id: TopicRouteData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.route;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TopicRouteData extends RemotingSerializable {
    private String orderTopicConf;
    private List<QueueData> queueDatas;
    private List<BrokerData> brokerDatas;
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public TopicRouteData cloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDatas(new ArrayList<QueueData>());
        topicRouteData.setBrokerDatas(new ArrayList<BrokerData>());
        topicRouteData.setFilterServerTable(new HashMap<String, List<String>>());
        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.queueDatas != null) {
            topicRouteData.getQueueDatas().addAll(this.queueDatas);
        }

        if (this.brokerDatas != null) {
            topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
        }

        if (this.filterServerTable != null) {
            topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
        }

        return topicRouteData;
    }

    public List<QueueData> getQueueDatas() {
        return queueDatas;
    }

    public void setQueueDatas(List<QueueData> queueDatas) {
        this.queueDatas = queueDatas;
    }

    public List<BrokerData> getBrokerDatas() {
        return brokerDatas;
    }

    public void setBrokerDatas(List<BrokerData> brokerDatas) {
        this.brokerDatas = brokerDatas;
    }

    public HashMap<String, List<String>> getFilterServerTable() {
        return filterServerTable;
    }

    public void setFilterServerTable(HashMap<String, List<String>> filterServerTable) {
        this.filterServerTable = filterServerTable;
    }

    public String getOrderTopicConf() {
        return orderTopicConf;
    }

    public void setOrderTopicConf(String orderTopicConf) {
        this.orderTopicConf = orderTopicConf;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((brokerDatas == null) ? 0 : brokerDatas.hashCode());
        result = prime * result + ((orderTopicConf == null) ? 0 : orderTopicConf.hashCode());
        result = prime * result + ((queueDatas == null) ? 0 : queueDatas.hashCode());
        result = prime * result + ((filterServerTable == null) ? 0 : filterServerTable.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        TopicRouteData other = (TopicRouteData) obj;
        if (brokerDatas == null) {
            if (other.brokerDatas != null) return false;
        } else if (!brokerDatas.equals(other.brokerDatas)) return false;
        if (orderTopicConf == null) {
            if (other.orderTopicConf != null) return false;
        } else if (!orderTopicConf.equals(other.orderTopicConf)) return false;
        if (queueDatas == null) {
            if (other.queueDatas != null) return false;
        } else if (!queueDatas.equals(other.queueDatas)) return false;
        if (filterServerTable == null) {
            if (other.filterServerTable != null) return false;
        } else if (!filterServerTable.equals(other.filterServerTable)) return false;
        return true;
    }

    @Override
    public String toString() {
        return "TopicRouteData [orderTopicConf=" + orderTopicConf + ", queueDatas=" + queueDatas + ", brokerDatas=" + brokerDatas + ", filterServerTable=" + filterServerTable + "]";
    }
}
