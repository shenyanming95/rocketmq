package org.apache.rocketmq.tools.command.connection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Iterator;
import java.util.Map.Entry;

public class ConsumerConnectionSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "consumerConnection";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's socket connection, client version and subscription";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
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

            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);

            int i = 1;
            for (Connection conn : cc.getConnectionSet()) {
                System.out.printf("%03d  %-32s %-22s %-8s %s%n", i++, conn.getClientId(), conn.getClientAddr(), conn.getLanguage(), MQVersion.getVersionDesc(conn.getVersion()));
            }

            System.out.printf("%nBelow is subscription:");
            Iterator<Entry<String, SubscriptionData>> it = cc.getSubscriptionTable().entrySet().iterator();
            i = 1;
            while (it.hasNext()) {
                Entry<String, SubscriptionData> entry = it.next();
                SubscriptionData sd = entry.getValue();
                System.out.printf("%03d  Topic: %-40s SubExpression: %s%n", i++, sd.getTopic(), sd.getSubString());
            }

            System.out.printf("");
            System.out.printf("ConsumeType: %s%n", cc.getConsumeType());
            System.out.printf("MessageModel: %s%n", cc.getMessageModel());
            System.out.printf("ConsumeFromWhere: %s%n", cc.getConsumeFromWhere());
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
