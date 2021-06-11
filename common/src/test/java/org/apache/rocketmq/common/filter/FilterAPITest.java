package org.apache.rocketmq.common.filter;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterAPITest {
    private String topic = "FooBar";
    private String group = "FooBarGroup";
    private String subString = "TAG1 || Tag2 || tag3";

    @Test
    public void testBuildSubscriptionData() throws Exception {
        SubscriptionData subscriptionData =
                FilterAPI.buildSubscriptionData(group, topic, subString);
        assertThat(subscriptionData.getTopic()).isEqualTo(topic);
        assertThat(subscriptionData.getSubString()).isEqualTo(subString);
        String[] tags = subString.split("\\|\\|");
        Set<String> tagSet = new HashSet<String>();
        for (String tag : tags) {
            tagSet.add(tag.trim());
        }
        assertThat(subscriptionData.getTagsSet()).isEqualTo(tagSet);
    }

    @Test
    public void testBuildTagSome() {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(
                    "TOPIC", "A || B", ExpressionType.TAG
            );

            assertThat(subscriptionData).isNotNull();
            assertThat(subscriptionData.getTopic()).isEqualTo("TOPIC");
            assertThat(subscriptionData.getSubString()).isEqualTo("A || B");
            assertThat(ExpressionType.isTagType(subscriptionData.getExpressionType())).isTrue();

            assertThat(subscriptionData.getTagsSet()).isNotNull();
            assertThat(subscriptionData.getTagsSet()).containsExactly("A", "B");
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test
    public void testBuildSQL() {
        try {
            SubscriptionData subscriptionData = FilterAPI.build(
                    "TOPIC", "a is not null", ExpressionType.SQL92
            );

            assertThat(subscriptionData).isNotNull();
            assertThat(subscriptionData.getTopic()).isEqualTo("TOPIC");
            assertThat(subscriptionData.getExpressionType()).isEqualTo(ExpressionType.SQL92);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildSQLWithNullSubString() throws Exception {
        FilterAPI.build("TOPIC", null, ExpressionType.SQL92);
    }
}
