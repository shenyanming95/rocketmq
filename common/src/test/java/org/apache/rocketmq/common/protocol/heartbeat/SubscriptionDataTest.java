package org.apache.rocketmq.common.protocol.heartbeat;

import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.assertj.core.util.Sets;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SubscriptionDataTest {

    @Test
    public void testConstructor1() {
        SubscriptionData subscriptionData = new SubscriptionData();
        assertThat(subscriptionData.getTopic()).isNull();
        assertThat(subscriptionData.getSubString()).isNull();
        assertThat(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        assertThat(subscriptionData.getExpressionType()).isEqualTo(ExpressionType.TAG);
        assertThat(subscriptionData.getFilterClassSource()).isNull();
        assertThat(subscriptionData.getCodeSet()).isEmpty();
        assertThat(subscriptionData.getTagsSet()).isEmpty();
        assertThat(subscriptionData.isClassFilterMode()).isFalse();
    }

    @Test
    public void testConstructor2() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        assertThat(subscriptionData.getTopic()).isEqualTo("TOPICA");
        assertThat(subscriptionData.getSubString()).isEqualTo("*");
        assertThat(subscriptionData.getSubVersion()).isLessThanOrEqualTo(System.currentTimeMillis());
        assertThat(subscriptionData.getExpressionType()).isEqualTo(ExpressionType.TAG);
        assertThat(subscriptionData.getFilterClassSource()).isNull();
        assertThat(subscriptionData.getCodeSet()).isEmpty();
        assertThat(subscriptionData.getTagsSet()).isEmpty();
        assertThat(subscriptionData.isClassFilterMode()).isFalse();
    }


    @Test
    public void testHashCodeNotEquals() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        assertThat(subscriptionData.hashCode()).isNotEqualTo(System.identityHashCode(subscriptionData));
    }

    @Test
    public void testFromJson() throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        subscriptionData.setFilterClassSource("TestFilterClassSource");
        subscriptionData.setCodeSet(Sets.newLinkedHashSet(1, 2, 3));
        subscriptionData.setTagsSet(Sets.newLinkedHashSet("TAGA", "TAGB", "TAG3"));
        String json = RemotingSerializable.toJson(subscriptionData, true);
        SubscriptionData fromJson = RemotingSerializable.fromJson(json, SubscriptionData.class);
        assertThat(subscriptionData).isEqualTo(fromJson);
        assertThat(subscriptionData).isEqualByComparingTo(fromJson);
        assertThat(subscriptionData.getFilterClassSource()).isEqualTo("TestFilterClassSource");
        assertThat(fromJson.getFilterClassSource()).isNull();
    }


    @Test
    public void testCompareTo() {
        SubscriptionData subscriptionData = new SubscriptionData("TOPICA", "*");
        SubscriptionData subscriptionData1 = new SubscriptionData("TOPICBA", "*");
        assertThat(subscriptionData.compareTo(subscriptionData1)).isEqualTo("TOPICA@*".compareTo("TOPICB@*"));
    }
}
