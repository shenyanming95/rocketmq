package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class DeleteTopicSubCommand implements SubCommand {
    public static void deleteTopic(final DefaultMQAdminExt adminExt, final String clusterName, final String topic) throws InterruptedException, MQBrokerException, RemotingException, MQClientException {

        Set<String> brokerAddressSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(adminExt, clusterName);
        adminExt.deleteTopicInBroker(brokerAddressSet, topic);
        System.out.printf("delete topic [%s] from cluster [%s] success.%n", topic, clusterName);

        Set<String> nameServerSet = null;
        if (adminExt.getNamesrvAddr() != null) {
            String[] ns = adminExt.getNamesrvAddr().trim().split(";");
            nameServerSet = new HashSet(Arrays.asList(ns));
        }

        adminExt.deleteTopicInNameServer(nameServerSet, topic);
        System.out.printf("delete topic [%s] from NameServer success.%n", topic);
    }

    @Override
    public String commandName() {
        return "deleteTopic";
    }

    @Override
    public String commandDesc() {
        return "Delete topic from broker and NameServer.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "delete topic from which cluster");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt adminExt = new DefaultMQAdminExt(rpcHook);
        adminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            String topic = commandLine.getOptionValue('t').trim();

            if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                adminExt.start();
                deleteTopic(adminExt, clusterName, topic);
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
