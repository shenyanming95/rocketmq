package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.JSON;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 可序列化的网络交互对象的抽象父类.
 * 本质上, 就是将对象JSON化, 然后再将字符串转为字节数组.
 */
public abstract class RemotingSerializable {

    private final static Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        return json.getBytes(CHARSET_UTF8);
    }

    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    public String toJson() {
        return toJson(false);
    }

    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}
