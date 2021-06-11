package org.apache.rocketmq.common.protocol.body;

import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;


public class ConsumeMessageDirectlyResultTest {
    @Test
    public void testFromJson() throws Exception {
        ConsumeMessageDirectlyResult result = new ConsumeMessageDirectlyResult();
        boolean defaultAutoCommit = true;
        boolean defaultOrder = false;
        long defaultSpentTimeMills = 1234567L;
        String defaultRemark = "defaultMark";
        CMResult defaultCMResult = CMResult.CR_COMMIT;

        result.setAutoCommit(defaultAutoCommit);
        result.setOrder(defaultOrder);
        result.setRemark(defaultRemark);
        result.setSpentTimeMills(defaultSpentTimeMills);
        result.setConsumeResult(defaultCMResult);

        String json = RemotingSerializable.toJson(result, true);
        ConsumeMessageDirectlyResult fromJson = RemotingSerializable.fromJson(json, ConsumeMessageDirectlyResult.class);
        assertThat(fromJson).isNotNull();

        assertThat(fromJson.getRemark()).isEqualTo(defaultRemark);
        assertThat(fromJson.getSpentTimeMills()).isEqualTo(defaultSpentTimeMills);
        assertThat(fromJson.getConsumeResult()).isEqualTo(defaultCMResult);
        assertThat(fromJson.isOrder()).isEqualTo(defaultOrder);

    }
}
