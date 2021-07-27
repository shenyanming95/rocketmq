package org.apache.rocketmq.tools.command.namesrv;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GetNamesrvConfigCommand implements SubCommand {

    @Override
    public String commandName() {
        return "getNamesrvConfig";
    }

    @Override
    public String commandDesc() {
        return "Get configs of name server.";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, final RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
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

            Map<String, Properties> nameServerConfigs = defaultMQAdminExt.getNameServerConfig(serverList);

            for (String server : nameServerConfigs.keySet()) {
                System.out.printf("============%s============\n", server);
                for (Object key : nameServerConfigs.get(server).keySet()) {
                    System.out.printf("%-50s=  %s\n", key, nameServerConfigs.get(server).get(key));
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}