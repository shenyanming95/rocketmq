package org.apache.rocketmq.tools.command.broker;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class BrokerStatusSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "brokerStatus";
    }

    @Override
    public String commandDesc() {
        return "Fetch broker runtime status data";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerAddr", true, "Broker address");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("c", "clusterName", true, "which cluster");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            String brokerAddr = commandLine.hasOption('b') ? commandLine.getOptionValue('b').trim() : null;
            String clusterName = commandLine.hasOption('c') ? commandLine.getOptionValue('c').trim() : null;
            if (brokerAddr != null) {
                printBrokerRuntimeStats(defaultMQAdminExt, brokerAddr, false);
            } else if (clusterName != null) {
                Set<String> masterSet = CommandUtil.fetchMasterAndSlaveAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String ba : masterSet) {
                    try {
                        printBrokerRuntimeStats(defaultMQAdminExt, ba, true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    public void printBrokerRuntimeStats(final DefaultMQAdminExt defaultMQAdminExt, final String brokerAddr, final boolean printBroker) throws InterruptedException, MQBrokerException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        KVTable kvTable = defaultMQAdminExt.fetchBrokerRuntimeStats(brokerAddr);

        TreeMap<String, String> tmp = new TreeMap<String, String>();
        tmp.putAll(kvTable.getTable());

        Iterator<Entry<String, String>> it = tmp.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> next = it.next();
            if (printBroker) {
                System.out.printf("%-24s %-32s: %s%n", brokerAddr, next.getKey(), next.getValue());
            } else {
                System.out.printf("%-32s: %s%n", next.getKey(), next.getValue());
            }
        }
    }
}
