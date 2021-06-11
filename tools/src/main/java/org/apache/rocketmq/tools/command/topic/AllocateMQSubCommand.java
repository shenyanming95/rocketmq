package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class AllocateMQSubCommand implements SubCommand {
    @Override
    public String commandName() {
        return "allocateMQ";
    }

    @Override
    public String commandDesc() {
        return "Allocate MQ";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "ipList", true, "ipList");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            adminExt.start();

            String topic = commandLine.getOptionValue('t').trim();
            String ips = commandLine.getOptionValue('i').trim();
            final String[] split = ips.split(",");
            final List<String> ipList = new LinkedList<String>();
            for (String ip : split) {
                ipList.add(ip);
            }

            final TopicRouteData topicRouteData = adminExt.examineTopicRouteInfo(topic);
            final Set<MessageQueue> mqs = MQClientInstance.topicRouteData2TopicSubscribeInfo(topic, topicRouteData);

            final AllocateMessageQueueAveragely averagely = new AllocateMessageQueueAveragely();

            RebalanceResult rr = new RebalanceResult();

            for (String i : ipList) {
                final List<MessageQueue> mqResult = averagely.allocate("aa", i, new ArrayList<MessageQueue>(mqs), ipList);
                rr.getResult().put(i, mqResult);
            }

            final String json = RemotingSerializable.toJson(rr, false);
            System.out.printf("%s%n", json);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
