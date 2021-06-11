package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class QueryCorrectionOffsetBodyTest {

    @Test
    public void testFromJson() throws Exception {
        QueryCorrectionOffsetBody qcob = new QueryCorrectionOffsetBody();
        Map<Integer, Long> offsetMap = new HashMap<Integer, Long>();
        offsetMap.put(1, 100L);
        offsetMap.put(2, 200L);
        qcob.setCorrectionOffsets(offsetMap);
        String json = RemotingSerializable.toJson(qcob, true);
        QueryCorrectionOffsetBody fromJson = RemotingSerializable.fromJson(json, QueryCorrectionOffsetBody.class);
        assertThat(fromJson.getCorrectionOffsets().get(1)).isEqualTo(100L);
        assertThat(fromJson.getCorrectionOffsets().get(2)).isEqualTo(200L);
        assertThat(fromJson.getCorrectionOffsets().size()).isEqualTo(2);
    }
}
