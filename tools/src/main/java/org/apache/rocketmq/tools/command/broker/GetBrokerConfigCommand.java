package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetBrokerConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "getBrokerConfig";
    }

    @Override
    public String commandDesc() {
        return "Get broker config by cluster or special broker!";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("b", "brokerAddr", true, "get which broker");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "get which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
                        final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {

            if (commandLine.hasOption('b')) {
                String brokerAddr = commandLine.getOptionValue('b').trim();
                defaultMQAdminExt.start();

                getAndPrint(defaultMQAdminExt,
                        String.format("============%s============\n", brokerAddr),
                        brokerAddr);

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();
                defaultMQAdminExt.start();

                Map<String, List<String>> masterAndSlaveMap
                        = CommandUtil.fetchMasterAndSlaveDistinguish(defaultMQAdminExt, clusterName);

                for (String masterAddr : masterAndSlaveMap.keySet()) {

                    getAndPrint(
                            defaultMQAdminExt,
                            String.format("============Master: %s============\n", masterAddr),
                            masterAddr
                    );
                    for (String slaveAddr : masterAndSlaveMap.get(masterAddr)) {

                        getAndPrint(
                                defaultMQAdminExt,
                                String.format("============My Master: %s=====Slave: %s============\n", masterAddr, slaveAddr),
                                slaveAddr
                        );
                    }
                }
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected void getAndPrint(final MQAdminExt defaultMQAdminExt, final String printPrefix, final String addr)
            throws InterruptedException, RemotingConnectException,
            UnsupportedEncodingException, RemotingTimeoutException,
            MQBrokerException, RemotingSendRequestException {

        System.out.print(printPrefix);

        Properties properties = defaultMQAdminExt.getBrokerConfig(addr);
        if (properties == null) {
            System.out.printf("Broker[%s] has no config property!\n", addr);
            return;
        }

        for (Object key : properties.keySet()) {
            System.out.printf("%-50s=  %s\n", key, properties.get(key));
        }

        System.out.printf("%n");
    }
}
