package org.apache.rocketmq.tools.command.acl;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Set;

public class UpdateGlobalWhiteAddrSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "updateGlobalWhiteAddr";
    }

    @Override
    public String commandDesc() {
        return "Update global white address for acl Config File in broker";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {

        OptionGroup optionGroup = new OptionGroup();

        Option opt = new Option("b", "brokerAddr", true, "update global white address to which broker");
        optionGroup.addOption(opt);

        opt = new Option("c", "clusterName", true, "update global white address to which cluster");
        optionGroup.addOption(opt);

        optionGroup.setRequired(true);
        options.addOptionGroup(optionGroup);

        opt = new Option("g", "globalWhiteRemoteAddresses", true, "set globalWhiteRemoteAddress list,eg: 10.10.103.*,192.168.0.*");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            // GlobalWhiteRemoteAddresses list value
            String globalWhiteRemoteAddresses = commandLine.getOptionValue('g').trim();


            if (commandLine.hasOption('b')) {
                String addr = commandLine.getOptionValue('b').trim();

                defaultMQAdminExt.start();
                defaultMQAdminExt.updateGlobalWhiteAddrConfig(addr, globalWhiteRemoteAddresses);

                System.out.printf("update global white remote addresses to %s success.%n", addr);
                return;

            } else if (commandLine.hasOption('c')) {
                String clusterName = commandLine.getOptionValue('c').trim();

                defaultMQAdminExt.start();
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
                for (String addr : masterSet) {
                    defaultMQAdminExt.updateGlobalWhiteAddrConfig(addr, globalWhiteRemoteAddresses);
                    System.out.printf("update global white remote addresses to %s success.%n", addr);
                }
                return;
            }

            ServerUtil.printCommandLineHelp("mqadmin " + this.commandName(), options);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
