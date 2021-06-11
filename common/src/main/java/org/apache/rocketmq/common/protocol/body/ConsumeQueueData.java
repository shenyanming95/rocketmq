package org.apache.rocketmq.common.protocol.body;

public class ConsumeQueueData {

    private long physicOffset;
    private int physicSize;
    private long tagsCode;
    private String extendDataJson;
    private String bitMap;
    private boolean eval;
    private String msg;

    public long getPhysicOffset() {
        return physicOffset;
    }

    public void setPhysicOffset(long physicOffset) {
        this.physicOffset = physicOffset;
    }

    public int getPhysicSize() {
        return physicSize;
    }

    public void setPhysicSize(int physicSize) {
        this.physicSize = physicSize;
    }

    public long getTagsCode() {
        return tagsCode;
    }

    public void setTagsCode(long tagsCode) {
        this.tagsCode = tagsCode;
    }

    public String getExtendDataJson() {
        return extendDataJson;
    }

    public void setExtendDataJson(String extendDataJson) {
        this.extendDataJson = extendDataJson;
    }

    public String getBitMap() {
        return bitMap;
    }

    public void setBitMap(String bitMap) {
        this.bitMap = bitMap;
    }

    public boolean isEval() {
        return eval;
    }

    public void setEval(boolean eval) {
        this.eval = eval;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "ConsumeQueueData{" +
                "physicOffset=" + physicOffset +
                ", physicSize=" + physicSize +
                ", tagsCode=" + tagsCode +
                ", extendDataJson='" + extendDataJson + '\'' +
                ", bitMap='" + bitMap + '\'' +
                ", eval=" + eval +
                ", msg='" + msg + '\'' +
                '}';
    }
}
