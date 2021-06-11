package org.apache.rocketmq.tools.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.remoting.RPCHook;

public interface SubCommand {
    String commandName();

    String commandDesc();

    Options buildCommandlineOptions(final Options options);

    void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException;
}
