package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class UpdateNamesrvConfigCommand implements SubCommand {
    @Override
    public String commandName() {
        return "updateNamesrvConfig";
    }

    @Override
    public String commandDesc() {
        return "Update configs of name server.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("k", "key", true, "config key");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("v", "value", true, "config value");
        opt.setRequired(true);
        options.addOption(opt);

        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
                        final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            // key name
            String key = commandLine.getOptionValue('k').trim();
            // key name
            String value = commandLine.getOptionValue('v').trim();
            Properties properties = new Properties();
            properties.put(key, value);

            // servers
            String servers = commandLine.getOptionValue('n');
            List<String> serverList = null;
            if (servers != null && servers.length() > 0) {
                String[] serverArray = servers.trim().split(";");

                if (serverArray.length > 0) {
                    serverList = Arrays.asList(serverArray);
                }
            }

            defaultMQAdminExt.start();

            defaultMQAdminExt.updateNameServerConfig(properties, serverList);

            System.out.printf("update name server config success!%s\n%s : %s\n",
                    serverList == null ? "" : serverList, key, value);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}