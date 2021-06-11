package org.apache.rocketmq.tools.command.connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class ProducerConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "producerConnection";
    }

    @Override
    public String commandDesc() {
        return "Query producer's socket connection and client version";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "producerGroup", true, "producer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
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

            String group = commandLine.getOptionValue('g').trim();
            String topic = commandLine.getOptionValue('t').trim();

            ProducerConnection pc = defaultMQAdminExt.examineProducerConnectionInfo(group, topic);

            int i = 1;
            for (Connection conn : pc.getConnectionSet()) {
                System.out.printf("%04d  %-32s %-22s %-8s %s%n",
                        i++,
                        conn.getClientId(),
                        conn.getClientAddr(),
                        conn.getLanguage(),
                        MQVersion.getVersionDesc(conn.getVersion())
                );
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
