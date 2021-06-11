package org.apache.rocketmq.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class DataVersionTest {

    @Test
    public void testEquals() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        Assert.assertTrue(dataVersion.equals(other));
    }

    @Test
    public void testEquals_falseWhenCounterDifferent() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(new AtomicLong(1L));
        other.setTimestamp(dataVersion.getTimestamp());
        Assert.assertFalse(dataVersion.equals(other));
    }

    @Test
    public void testEquals_falseWhenCounterDifferent2() {
        DataVersion dataVersion = new DataVersion();
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        Assert.assertFalse(dataVersion.equals(other));
    }

    @Test
    public void testEquals_falseWhenCounterDifferent3() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setTimestamp(dataVersion.getTimestamp());
        Assert.assertFalse(dataVersion.equals(other));
    }

    @Test
    public void testEquals_trueWhenCountersBothNull() {
        DataVersion dataVersion = new DataVersion();
        dataVersion.setCounter(null);
        DataVersion other = new DataVersion();
        other.setCounter(null);
        other.setTimestamp(dataVersion.getTimestamp());
        Assert.assertTrue(dataVersion.equals(other));
    }
}