package org.apache.rocketmq.client.trace.hook;

import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.trace.*;
import org.apache.rocketmq.common.protocol.NamespaceUtil;

import java.util.ArrayList;

public class SendMessageTraceHookImpl implements SendMessageHook {

    private TraceDispatcher localDispatcher;

    public SendMessageTraceHookImpl(TraceDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public String hookName() {
        return "SendMessageTraceHook";
    }

    @Override
    public void sendMessageBefore(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName())) {
            return;
        }
        //build the context content of TuxeTraceContext
        TraceContext tuxeContext = new TraceContext();
        tuxeContext.setTraceBeans(new ArrayList<TraceBean>(1));
        context.setMqTraceContext(tuxeContext);
        tuxeContext.setTraceType(TraceType.Pub);
        tuxeContext.setGroupName(NamespaceUtil.withoutNamespace(context.getProducerGroup()));
        //build the data bean object of message trace
        TraceBean traceBean = new TraceBean();
        traceBean.setTopic(NamespaceUtil.withoutNamespace(context.getMessage().getTopic()));
        traceBean.setTags(context.getMessage().getTags());
        traceBean.setKeys(context.getMessage().getKeys());
        traceBean.setStoreHost(context.getBrokerAddr());
        traceBean.setBodyLength(context.getMessage().getBody().length);
        traceBean.setMsgType(context.getMsgType());
        traceBean.setClientHost(((AsyncTraceDispatcher) localDispatcher).getHostProducer().getmQClientFactory().getClientId());
        tuxeContext.getTraceBeans().add(traceBean);
    }

    @Override
    public void sendMessageAfter(SendMessageContext context) {
        //if it is message trace data,then it doesn't recorded
        if (context == null || context.getMessage().getTopic().startsWith(((AsyncTraceDispatcher) localDispatcher).getTraceTopicName()) || context.getMqTraceContext() == null) {
            return;
        }
        if (context.getSendResult() == null) {
            return;
        }

        if (context.getSendResult().getRegionId() == null || !context.getSendResult().isTraceOn()) {
            // if switch is false,skip it
            return;
        }

        TraceContext tuxeContext = (TraceContext) context.getMqTraceContext();
        TraceBean traceBean = tuxeContext.getTraceBeans().get(0);
        int costTime = (int) ((System.currentTimeMillis() - tuxeContext.getTimeStamp()) / tuxeContext.getTraceBeans().size());
        tuxeContext.setCostTime(costTime);
        if (context.getSendResult().getSendStatus().equals(SendStatus.SEND_OK)) {
            tuxeContext.setSuccess(true);
        } else {
            tuxeContext.setSuccess(false);
        }
        tuxeContext.setRegionId(context.getSendResult().getRegionId());
        traceBean.setMsgId(context.getSendResult().getMsgId());
        traceBean.setOffsetMsgId(context.getSendResult().getOffsetMsgId());
        traceBean.setStoreTime(tuxeContext.getTimeStamp() + costTime / 2);
        localDispatcher.append(tuxeContext);
    }
}
