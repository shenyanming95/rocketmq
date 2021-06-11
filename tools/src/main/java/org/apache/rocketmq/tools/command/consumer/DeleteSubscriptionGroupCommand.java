package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.command.topic.DeleteTopicSubCommand;

import java.util.Set;

public class DeleteSubscriptionGroupCommand implements SubCommand {
    @Override
    public String commandName() {
        return "deleteSubGroup";
    }

    @Override
    public String commandDesc() {
        return "Delete subscription group from broker.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "delete subscription group from which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete subscription group from which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("g", "groupName", true, "subscription group name");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // groupName
            String groupName = commandLine.getOptionValue('g').trim();

            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();
                adminExt.start();

                adminExt.deleteSubscriptionGroup(addr, groupName);
                System.out.printf("delete subscription group [%s] from broker [%s] success.%n", groupName,
                        addr);

                return;
            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                adminExt.start();

                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(adminExt, clusterName);
                for (String master : masterSet) {
                    adminExt.deleteSubscriptionGroup(master, groupName);
                    System.out.printf(
                            "delete subscription group [%s] from broker [%s] in cluster [%s] success.%n",
                            groupName, master, clusterName);
                }

                try {
                    DeleteTopicSubCommand.deleteTopic(adminExt, clusterName, MixAll.RETRY_GROUP_TOPIC_PREFIX
                            + groupName);
                    DeleteTopicSubCommand.deleteTopic(adminExt, clusterName, MixAll.DLQ_GROUP_TOPIC_PREFIX
                            + groupName);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            adminExt.shutdown();
        }
    }
}
