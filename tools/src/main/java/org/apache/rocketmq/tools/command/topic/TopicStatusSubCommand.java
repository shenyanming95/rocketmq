package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class TopicStatusSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicStatus";
    }

    @Override
    public String commandDesc() {
        return "Examine topic Status info";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String topic = commandLine.getOptionValue('t').trim();
            TopicStatsTable topicStatsTable = defaultMQAdminExt.examineTopicStats(topic);

            List<MessageQueue> mqList = new LinkedList<MessageQueue>();
            mqList.addAll(topicStatsTable.getOffsetTable().keySet());
            Collections.sort(mqList);

            System.out.printf("%-32s  %-4s  %-20s  %-20s    %s%n", "#Broker Name", "#QID", "#Min Offset", "#Max Offset", "#Last Updated");

            for (MessageQueue mq : mqList) {
                TopicOffset topicOffset = topicStatsTable.getOffsetTable().get(mq);

                String humanTimestamp = "";
                if (topicOffset.getLastUpdateTimestamp() > 0) {
                    humanTimestamp = UtilAll.timeMillisToHumanString2(topicOffset.getLastUpdateTimestamp());
                }

                System.out.printf("%-32s  %-4d  %-20d  %-20d    %s%n", UtilAll.frontStringAtLeast(mq.getBrokerName(), 32), mq.getQueueId(), topicOffset.getMinOffset(), topicOffset.getMaxOffset(), humanTimestamp);
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
