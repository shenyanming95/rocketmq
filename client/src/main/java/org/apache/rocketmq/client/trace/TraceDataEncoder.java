package org.apache.rocketmq.client.trace;

import org.apache.rocketmq.common.message.MessageType;

import java.util.ArrayList;
import java.util.List;

/**
 * Encode/decode for Trace Data
 */
public class TraceDataEncoder {

    /**
     * Resolving traceContext list From trace data String
     *
     * @param traceData
     * @return
     */
    public static List<TraceContext> decoderFromTraceDataString(String traceData) {
        List<TraceContext> resList = new ArrayList<TraceContext>();
        if (traceData == null || traceData.length() <= 0) {
            return resList;
        }
        String[] contextList = traceData.split(String.valueOf(TraceConstants.FIELD_SPLITOR));
        for (String context : contextList) {
            String[] line = context.split(String.valueOf(TraceConstants.CONTENT_SPLITOR));
            if (line[0].equals(TraceType.Pub.name())) {
                TraceContext pubContext = new TraceContext();
                pubContext.setTraceType(TraceType.Pub);
                pubContext.setTimeStamp(Long.parseLong(line[1]));
                pubContext.setRegionId(line[2]);
                pubContext.setGroupName(line[3]);
                TraceBean bean = new TraceBean();
                bean.setTopic(line[4]);
                bean.setMsgId(line[5]);
                bean.setTags(line[6]);
                bean.setKeys(line[7]);
                bean.setStoreHost(line[8]);
                bean.setBodyLength(Integer.parseInt(line[9]));
                pubContext.setCostTime(Integer.parseInt(line[10]));
                bean.setMsgType(MessageType.values()[Integer.parseInt(line[11])]);

                if (line.length == 13) {
                    pubContext.setSuccess(Boolean.parseBoolean(line[12]));
                } else if (line.length == 14) {
                    bean.setOffsetMsgId(line[12]);
                    pubContext.setSuccess(Boolean.parseBoolean(line[13]));
                }

                // compatible with the old version
                if (line.length >= 15) {
                    bean.setOffsetMsgId(line[12]);
                    pubContext.setSuccess(Boolean.parseBoolean(line[13]));
                    bean.setClientHost(line[14]);
                }

                pubContext.setTraceBeans(new ArrayList<TraceBean>(1));
                pubContext.getTraceBeans().add(bean);
                resList.add(pubContext);
            } else if (line[0].equals(TraceType.SubBefore.name())) {
                TraceContext subBeforeContext = new TraceContext();
                subBeforeContext.setTraceType(TraceType.SubBefore);
                subBeforeContext.setTimeStamp(Long.parseLong(line[1]));
                subBeforeContext.setRegionId(line[2]);
                subBeforeContext.setGroupName(line[3]);
                subBeforeContext.setRequestId(line[4]);
                TraceBean bean = new TraceBean();
                bean.setMsgId(line[5]);
                bean.setRetryTimes(Integer.parseInt(line[6]));
                bean.setKeys(line[7]);
                bean.setClientHost(line[8]);
                subBeforeContext.setTraceBeans(new ArrayList<TraceBean>(1));
                subBeforeContext.getTraceBeans().add(bean);
                resList.add(subBeforeContext);
            } else if (line[0].equals(TraceType.SubAfter.name())) {
                TraceContext subAfterContext = new TraceContext();
                subAfterContext.setTraceType(TraceType.SubAfter);
                subAfterContext.setRequestId(line[1]);
                TraceBean bean = new TraceBean();
                bean.setMsgId(line[2]);
                bean.setKeys(line[5]);
                subAfterContext.setTraceBeans(new ArrayList<TraceBean>(1));
                subAfterContext.getTraceBeans().add(bean);
                subAfterContext.setCostTime(Integer.parseInt(line[3]));
                subAfterContext.setSuccess(Boolean.parseBoolean(line[4]));
                if (line.length >= 7) {
                    // add the context type
                    subAfterContext.setContextCode(Integer.parseInt(line[6]));
                }
                // compatible with the old version
                if (line.length >= 9) {
                    subAfterContext.setTimeStamp(Long.parseLong(line[7]));
                    subAfterContext.setGroupName(line[8]);
                }
                resList.add(subAfterContext);
            }
        }
        return resList;
    }

    /**
     * Encoding the trace context into data strings and keyset sets
     *
     * @param ctx
     * @return
     */
    public static TraceTransferBean encoderFromContextBean(TraceContext ctx) {
        if (ctx == null) {
            return null;
        }
        //build message trace of the transfering entity content bean
        TraceTransferBean transferBean = new TraceTransferBean();
        StringBuilder sb = new StringBuilder(256);
        switch (ctx.getTraceType()) {
            case Pub: {
                TraceBean bean = ctx.getTraceBeans().get(0);
                //append the content of context and traceBean to transferBean's TransData
                sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getTopic()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getTags()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getStoreHost()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getBodyLength()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getMsgType().ordinal()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getOffsetMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(ctx.isSuccess()).append(TraceConstants.CONTENT_SPLITOR)//
                        .append(bean.getClientHost()).append(TraceConstants.FIELD_SPLITOR);
            }
            break;
            case SubBefore: {
                for (TraceBean bean : ctx.getTraceBeans()) {
                    sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getRegionId()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getGroupName()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getRetryTimes()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getClientHost()).append(TraceConstants.FIELD_SPLITOR);//
                }
            }
            break;
            case SubAfter: {
                for (TraceBean bean : ctx.getTraceBeans()) {
                    sb.append(ctx.getTraceType()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getRequestId()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getMsgId()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getCostTime()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.isSuccess()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(bean.getKeys()).append(TraceConstants.CONTENT_SPLITOR)//
                            .append(ctx.getContextCode()).append(TraceConstants.CONTENT_SPLITOR).append(ctx.getTimeStamp()).append(TraceConstants.CONTENT_SPLITOR).append(ctx.getGroupName()).append(TraceConstants.FIELD_SPLITOR);

                }
            }
            break;
            default:
        }
        transferBean.setTransData(sb.toString());
        for (TraceBean bean : ctx.getTraceBeans()) {

            transferBean.getTransKey().add(bean.getMsgId());
            if (bean.getKeys() != null && bean.getKeys().length() > 0) {
                transferBean.getTransKey().add(bean.getKeys());
            }
        }
        return transferBean;
    }
}
