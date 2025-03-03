package org.apache.rocketmq.tools.command.consumer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.MQAdminStartup;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

public class ConsumerSubCommand implements SubCommand {

    public static void main(String[] args) {
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        MQAdminStartup.main(new String[]{new ConsumerSubCommand().commandName(), "-g", "benchmark_consumer"});
    }

    @Override
    public String commandName() {
        return "consumer";
    }

    @Override
    public String commandDesc() {
        return "Query consumer's connection, status, etc.";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("g", "consumerGroup", true, "consumer group name");
        opt.setRequired(true);
        options.addOption(opt);

        opt = new Option("s", "jstack", false, "Run jstack command in the consumer progress");
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
            String group = commandLine.getOptionValue('g').trim();
            ConsumerConnection cc = defaultMQAdminExt.examineConsumerConnectionInfo(group);
            boolean jstack = commandLine.hasOption('s');

            if (!commandLine.hasOption('i')) {

                int i = 1;
                long now = System.currentTimeMillis();
                final TreeMap<String/* clientId */, ConsumerRunningInfo> criTable = new TreeMap<String, ConsumerRunningInfo>();
                for (Connection conn : cc.getConnectionSet()) {
                    try {
                        ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(group, conn.getClientId(), jstack);
                        if (consumerRunningInfo != null) {
                            criTable.put(conn.getClientId(), consumerRunningInfo);
                            String filePath = now + "/" + conn.getClientId();
                            MixAll.string2FileNotSafe(consumerRunningInfo.formatString(), filePath);
                            System.out.printf("%03d  %-40s %-20s %s%n", i++, conn.getClientId(), MQVersion.getVersionDesc(conn.getVersion()), filePath);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                if (!criTable.isEmpty()) {
                    boolean subSame = ConsumerRunningInfo.analyzeSubscription(criTable);
                    boolean rebalanceOK = subSame && ConsumerRunningInfo.analyzeRebalance(criTable);

                    if (subSame) {
                        System.out.printf("%n%nSame subscription in the same group of consumer");
                        System.out.printf("%n%nRebalance %s%n", rebalanceOK ? "OK" : "Failed");

                        Iterator<Entry<String, ConsumerRunningInfo>> it = criTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, ConsumerRunningInfo> next = it.next();
                            String result = ConsumerRunningInfo.analyzeProcessQueue(next.getKey(), next.getValue());
                            if (result.length() > 0) {
                                System.out.printf("%s", result);
                            }
                        }
                    } else {
                        System.out.printf("%n%nWARN: Different subscription in the same group of consumer!!!");
                    }
                }
            } else {
                String clientId = commandLine.getOptionValue('i').trim();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(group, clientId, jstack);
                if (consumerRunningInfo != null) {
                    System.out.printf("%s", consumerRunningInfo.formatString());
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
