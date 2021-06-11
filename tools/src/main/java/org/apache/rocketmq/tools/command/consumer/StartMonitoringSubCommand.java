package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;
import org.apache.rocketmq.tools.monitor.DefaultMonitorListener;
import org.apache.rocketmq.tools.monitor.MonitorConfig;
import org.apache.rocketmq.tools.monitor.MonitorService;

public class StartMonitoringSubCommand implements SubCommand {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public String commandName() {
        return "startMonitoring";
    }

    @Override
    public String commandDesc() {
        return "Start Monitoring";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        try {
            MonitorService monitorService =
                    new MonitorService(new MonitorConfig(), new DefaultMonitorListener(), rpcHook);

            monitorService.start();
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }
}
