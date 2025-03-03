package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.UnsupportedEncodingException;

public class SendMsgStatusCommand implements SubCommand {

    private static Message buildMessage(final String topic, final int messageSize) throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(topic);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messageSize; i += 11) {
            sb.append("hello jodie");
        }
        msg.setBody(sb.toString().getBytes(MixAll.DEFAULT_CHARSET));
        return msg;
    }

    @Override
    public String commandName() {
        return "sendMsgStatus";
    }

    @Override
    public String commandDesc() {
        return "send msg to broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerName", true, "Broker Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "messageSize", true, "Message Size, Default: 128");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "count", true, "send message count, Default: 50");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        final DefaultMQProducer producer = new DefaultMQProducer("PID_SMSC", rpcHook);
        producer.setInstanceName("PID_SMSC_" + System.currentTimeMillis());

        try {
            producer.start();
            String brokerName = commandLine.getOptionValue('b').trim();
            int messageSize = commandLine.hasOption('s') ? Integer.parseInt(commandLine.getOptionValue('s')) : 128;
            int count = commandLine.hasOption('c') ? Integer.parseInt(commandLine.getOptionValue('c')) : 50;

            producer.send(buildMessage(brokerName, 16));

            for (int i = 0; i < count; i++) {
                long begin = System.currentTimeMillis();
                SendResult result = producer.send(buildMessage(brokerName, messageSize));
                System.out.printf("rt:" + (System.currentTimeMillis() - begin) + "ms, SendResult=%s", result);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
        }
    }
}
