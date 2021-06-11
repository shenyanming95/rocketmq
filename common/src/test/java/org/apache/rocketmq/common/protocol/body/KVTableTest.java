package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;

public class KVTableTest {

    @Test
    public void testFromJson() throws Exception {
        HashMap<String, String> table = new HashMap<String, String>();
        table.put("key1", "value1");
        table.put("key2", "value2");

        KVTable kvTable = new KVTable();
        kvTable.setTable(table);

        String json = RemotingSerializable.toJson(kvTable, true);
        KVTable fromJson = RemotingSerializable.fromJson(json, KVTable.class);

        assertThat(fromJson).isNotEqualTo(kvTable);
        assertThat(fromJson.getTable().get("key1")).isEqualTo(kvTable.getTable().get("key1"));
        assertThat(fromJson.getTable().get("key2")).isEqualTo(kvTable.getTable().get("key2"));
    }

}
