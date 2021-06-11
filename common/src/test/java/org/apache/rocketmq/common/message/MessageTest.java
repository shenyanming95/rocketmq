package org.apache.rocketmq.common.message;

import org.junit.Assert;
import org.junit.Test;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_TRACE_SWITCH;

public class MessageTest {
    @Test(expected = RuntimeException.class)
    public void putUserPropertyWithRuntimeException() throws Exception {
        Message m = new Message();

        m.putUserProperty(PROPERTY_TRACE_SWITCH, "");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserNullValuePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserEmptyValuePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", "   ");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserNullNamePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty(null, "val1");
    }

    @Test(expected = IllegalArgumentException.class)
    public void putUserEmptyNamePropertyWithException() throws Exception {
        Message m = new Message();

        m.putUserProperty("   ", "val1");
    }

    @Test
    public void putUserProperty() throws Exception {
        Message m = new Message();

        m.putUserProperty("prop1", "val1");
        Assert.assertEquals("val1", m.getUserProperty("prop1"));
    }
}
