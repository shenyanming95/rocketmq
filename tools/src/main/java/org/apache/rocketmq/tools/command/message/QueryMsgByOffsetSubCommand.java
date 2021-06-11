package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class QueryMsgByOffsetSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "queryMsgByOffset";
    }

    @Override
    public String commandDesc() {
        return "Query Message by offset";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("b", "brokerName", true, "Broker Name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("i", "queueId", true, "Queue Id");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("o", "offset", true, "Queue Offset");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        DefaultMQPullConsumer defaultMQPullConsumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQPullConsumer.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            String topic = commandLine.getOptionValue('t').trim();
            String brokerName = commandLine.getOptionValue('b').trim();
            String queueId = commandLine.getOptionValue('i').trim();
            String offset = commandLine.getOptionValue('o').trim();

            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(brokerName);
            mq.setQueueId(Integer.parseInt(queueId));

            defaultMQPullConsumer.start();
            defaultMQAdminExt.start();

            PullResult pullResult = defaultMQPullConsumer.pull(mq, "*", Long.parseLong(offset), 1);
            if (pullResult != null) {
                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        QueryMsgByIdSubCommand.printMsg(defaultMQAdminExt, pullResult.getMsgFoundList().get(0));
                        break;
                    case NO_MATCHED_MSG:
                    case NO_NEW_MSG:
                    case OFFSET_ILLEGAL:
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQPullConsumer.shutdown();
            defaultMQAdminExt.shutdown();
        }
    }
}
