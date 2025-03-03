package org.apache.rocketmq.store;

import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.message.MessageExt;

public class MessageExtBrokerInner extends MessageExt {
    private static final long serialVersionUID = 7256001576878700634L;

    /**
     * producer 发送消息时携带过来的配置信息, 注意这边的格式为字符串(未转化过). 区别于
     * {@link org.apache.rocketmq.common.message.Message#properties}的map结构
     */
    private String propertiesString;

    private long tagsCode;

    public static long tagsString2tagsCode(final TopicFilterType filter, final String tags) {
        if (null == tags || tags.length() == 0) {
            return 0;
        }
        return tags.hashCode();
    }

    public static long tagsString2tagsCode(final String tags) {
        return tagsString2tagsCode(null, tags);
    }

    public String getPropertiesString() {
        return propertiesString;
    }

    public void setPropertiesString(String propertiesString) {
        this.propertiesString = propertiesString;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }
}
