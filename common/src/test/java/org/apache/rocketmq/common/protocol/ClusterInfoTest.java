package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ClusterInfoTest {

    @Test
    public void testFormJson() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertNotNull(json);
        assertNotNull(json.getClusterAddrTable());
        assertTrue(json.getClusterAddrTable().containsKey("DEFAULT_CLUSTER"));
        assertTrue(json.getClusterAddrTable().get("DEFAULT_CLUSTER").contains("master"));
        assertNotNull(json.getBrokerAddrTable());
        assertTrue(json.getBrokerAddrTable().containsKey("master"));
        assertEquals(json.getBrokerAddrTable().get("master").getBrokerName(), "master");
        assertEquals(json.getBrokerAddrTable().get("master").getCluster(), "DEFAULT_CLUSTER");
        assertEquals(json.getBrokerAddrTable().get("master").getBrokerAddrs().get(MixAll.MASTER_ID), MixAll.getLocalhostByNetworkInterface());
    }

    @Test
    public void testRetrieveAllClusterNames() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertArrayEquals(new String[]{"DEFAULT_CLUSTER"}, json.retrieveAllClusterNames());
    }


    @Test
    public void testRetrieveAllAddrByCluster() throws Exception {
        ClusterInfo clusterInfo = buildClusterInfo();
        byte[] data = clusterInfo.encode();
        ClusterInfo json = RemotingSerializable.decode(data, ClusterInfo.class);

        assertArrayEquals(new String[]{MixAll.getLocalhostByNetworkInterface()}, json.retrieveAllAddrByCluster("DEFAULT_CLUSTER"));
    }


    private ClusterInfo buildClusterInfo() throws Exception {
        ClusterInfo clusterInfo = new ClusterInfo();
        HashMap<String, BrokerData> brokerAddrTable = new HashMap<String, BrokerData>();
        HashMap<String, Set<String>> clusterAddrTable = new HashMap<String, Set<String>>();

        //build brokerData
        BrokerData brokerData = new BrokerData();
        brokerData.setBrokerName("master");
        brokerData.setCluster("DEFAULT_CLUSTER");

        //build brokerAddrs
        HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
        brokerAddrs.put(MixAll.MASTER_ID, MixAll.getLocalhostByNetworkInterface());

        brokerData.setBrokerAddrs(brokerAddrs);
        brokerAddrTable.put("master", brokerData);

        Set<String> brokerNames = new HashSet<String>();
        brokerNames.add("master");

        clusterAddrTable.put("DEFAULT_CLUSTER", brokerNames);

        clusterInfo.setBrokerAddrTable(brokerAddrTable);
        clusterInfo.setClusterAddrTable(clusterAddrTable);
        return clusterInfo;
    }
}