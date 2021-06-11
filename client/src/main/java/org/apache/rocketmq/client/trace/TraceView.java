package org.apache.rocketmq.client.trace;


import java.util.ArrayList;
import java.util.List;

public class TraceView {

    private String msgId;
    private String tags;
    private String keys;
    private String storeHost;
    private String clientHost;
    private int costTime;
    private String msgType;
    private String offSetMsgId;
    private long timeStamp;
    private long bornTime;
    private String topic;
    private String groupName;
    private String status;

    public static List<TraceView> decodeFromTraceTransData(String key, String messageBody) {
        List<TraceView> messageTraceViewList = new ArrayList<TraceView>();
        if (messageBody == null || messageBody.length() <= 0) {
            return messageTraceViewList;
        }

        List<TraceContext> traceContextList = TraceDataEncoder.decoderFromTraceDataString(messageBody);

        for (TraceContext context : traceContextList) {
            TraceView messageTraceView = new TraceView();
            TraceBean traceBean = context.getTraceBeans().get(0);
            if (!traceBean.getMsgId().equals(key)) {
                continue;
            }
            messageTraceView.setCostTime(context.getCostTime());
            messageTraceView.setGroupName(context.getGroupName());
            if (context.isSuccess()) {
                messageTraceView.setStatus("success");
            } else {
                messageTraceView.setStatus("failed");
            }
            messageTraceView.setKeys(traceBean.getKeys());
            messageTraceView.setMsgId(traceBean.getMsgId());
            messageTraceView.setTags(traceBean.getTags());
            messageTraceView.setTopic(traceBean.getTopic());
            messageTraceView.setMsgType(context.getTraceType().name());
            messageTraceView.setOffSetMsgId(traceBean.getOffsetMsgId());
            messageTraceView.setTimeStamp(context.getTimeStamp());
            messageTraceView.setStoreHost(traceBean.getStoreHost());
            messageTraceView.setClientHost(traceBean.getClientHost());
            messageTraceViewList.add(messageTraceView);
        }
        return messageTraceViewList;
    }

    public String getMsgId() {
        return msgId;
    }

    public void setMsgId(String msgId) {
        this.msgId = msgId;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getKeys() {
        return keys;
    }

    public void setKeys(String keys) {
        this.keys = keys;
    }

    public String getStoreHost() {
        return storeHost;
    }

    public void setStoreHost(String storeHost) {
        this.storeHost = storeHost;
    }

    public String getClientHost() {
        return clientHost;
    }

    public void setClientHost(String clientHost) {
        this.clientHost = clientHost;
    }

    public int getCostTime() {
        return costTime;
    }

    public void setCostTime(int costTime) {
        this.costTime = costTime;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getOffSetMsgId() {
        return offSetMsgId;
    }

    public void setOffSetMsgId(String offSetMsgId) {
        this.offSetMsgId = offSetMsgId;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public long getBornTime() {
        return bornTime;
    }

    public void setBornTime(long bornTime) {
        this.bornTime = bornTime;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}