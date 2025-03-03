package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class SendMessageCommand implements SubCommand {

    private DefaultMQProducer producer;

    @Override
    public String commandName() {
        return "sendMessage";
    }

    @Override
    public String commandDesc() {
        return "Send a message";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "Topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("p", "body", true, "UTF-8 string format of the message body");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("k", "key", true, "Message keys");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "tags", true, "Message tags");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("b", "broker", true, "Send message to target broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("i", "qid", true, "Send message to target queue");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    private DefaultMQProducer createProducer(RPCHook rpcHook) {
        if (this.producer != null) {
            return producer;
        } else {
            producer = new DefaultMQProducer(rpcHook);
            producer.setProducerGroup(Long.toString(System.currentTimeMillis()));
            return producer;
        }
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        Message msg = null;
        String topic = commandLine.getOptionValue('t').trim();
        String body = commandLine.getOptionValue('p').trim();
        String tag = null;
        String keys = null;
        String brokerName = null;
        int queueId = -1;
        try {
            if (commandLine.hasOption('k')) {
                keys = commandLine.getOptionValue('k').trim();
            }
            if (commandLine.hasOption('c')) {
                tag = commandLine.getOptionValue('c').trim();
            }
            if (commandLine.hasOption('b')) {
                brokerName = commandLine.getOptionValue('b').trim();
            }
            if (commandLine.hasOption('i')) {
                if (!commandLine.hasOption('b')) {
                    System.out.print("Broker name must be set if the queue is chosen!");
                    return;
                } else {
                    queueId = Integer.parseInt(commandLine.getOptionValue('i').trim());
                }
            }
            msg = new Message(topic, tag, keys, body.getBytes("utf-8"));
        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        }

        DefaultMQProducer producer = this.createProducer(rpcHook);
        SendResult result;
        try {
            producer.start();
            if (brokerName != null && queueId > -1) {
                MessageQueue messageQueue = new MessageQueue(topic, brokerName, queueId);
                result = producer.send(msg, messageQueue);
            } else {
                result = producer.send(msg);
            }

        } catch (Exception e) {
            throw new RuntimeException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            producer.shutdown();
        }

        System.out.printf("%-32s  %-4s  %-20s    %s%n", "#Broker Name", "#QID", "#Send Result", "#MsgId");

        if (result != null) {
            System.out.printf("%-32s  %-4s  %-20s    %s%n", result.getMessageQueue().getBrokerName(), result.getMessageQueue().getQueueId(), result.getSendStatus(), result.getMsgId());
        } else {
            System.out.printf("%-32s  %-4s  %-20s    %s%n", "Unknown", "Unknown", "Failed", "None");
        }
    }
}