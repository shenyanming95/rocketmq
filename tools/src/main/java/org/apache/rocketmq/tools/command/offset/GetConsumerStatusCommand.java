package org.apache.rocketmq.tools.command.offset;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Map;

public class GetConsumerStatusCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getConsumerStatus";
    }

    @Override
    public String commandDesc() {
        return "get consumer status from client.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "group", true, "set the consumer group");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "set the topic");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "originClientId", true, "set the consumer clientId");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String group = commandLine.getOptionValue("g").trim();
            String topic = commandLine.getOptionValue("t").trim();
            String originClientId = "";
            if (commandLine.hasOption("i")) {
                originClientId = commandLine.getOptionValue("i").trim();
            }
            defaultMQAdminExt.start();

            Map<String, Map<MessageQueue, Long>> consumerStatusTable = defaultMQAdminExt.getConsumeStatus(topic, group, originClientId);
            System.out.printf("get consumer status from client. group=%s, topic=%s, originClientId=%s%n", group, topic, originClientId);

            System.out.printf("%-50s  %-15s  %-15s  %-20s%n", "#clientId", "#brokerName", "#queueId", "#offset");

            for (Map.Entry<String, Map<MessageQueue, Long>> entry : consumerStatusTable.entrySet()) {
                String clientId = entry.getKey();
                Map<MessageQueue, Long> mqTable = entry.getValue();
                for (Map.Entry<MessageQueue, Long> entry1 : mqTable.entrySet()) {
                    MessageQueue mq = entry1.getKey();
                    System.out.printf("%-50s  %-15s  %-15d  %-20d%n", UtilAll.frontStringAtLeast(clientId, 50), mq.getBrokerName(), mq.getQueueId(), mqTable.get(mq));
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
