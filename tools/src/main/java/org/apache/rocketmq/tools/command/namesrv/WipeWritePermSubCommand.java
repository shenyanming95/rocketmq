package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.List;

public class WipeWritePermSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "wipeWritePerm";
    }

    @Override
    public String commandDesc() {
        return "Wipe write perm of broker in all name server";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("b", "brokerName", true, "broker name");
        opt.setRequired(true);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            String brokerName = commandLine.getOptionValue('b').trim();
            List<String> namesrvList = defaultMQAdminExt.getNameServerAddressList();
            if (namesrvList != null) {
                for (String namesrvAddr : namesrvList) {
                    try {
                        int wipeTopicCount = defaultMQAdminExt.wipeWritePermOfBroker(namesrvAddr, brokerName);
                        System.out.printf("wipe write perm of broker[%s] in name server[%s] OK, %d%n",
                                brokerName,
                                namesrvAddr,
                                wipeTopicCount
                        );
                    } catch (Exception e) {
                        System.out.printf("wipe write perm of broker[%s] in name server[%s] Failed%n",
                                brokerName,
                                namesrvAddr
                        );

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
}
