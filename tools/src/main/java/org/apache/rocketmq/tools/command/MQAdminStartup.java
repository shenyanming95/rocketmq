package org.apache.rocketmq.tools.command;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.acl.common.AclUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.command.acl.*;
import org.apache.rocketmq.tools.command.broker.*;
import org.apache.rocketmq.tools.command.cluster.CLusterSendMsgRTCommand;
import org.apache.rocketmq.tools.command.cluster.ClusterListSubCommand;
import org.apache.rocketmq.tools.command.connection.ConsumerConnectionSubCommand;
import org.apache.rocketmq.tools.command.connection.ProducerConnectionSubCommand;
import org.apache.rocketmq.tools.command.consumer.*;
import org.apache.rocketmq.tools.command.message.*;
import org.apache.rocketmq.tools.command.namesrv.*;
import org.apache.rocketmq.tools.command.offset.CloneGroupOffsetCommand;
import org.apache.rocketmq.tools.command.offset.ResetOffsetByTimeCommand;
import org.apache.rocketmq.tools.command.queue.QueryConsumeQueueCommand;
import org.apache.rocketmq.tools.command.stats.StatsAllSubCommand;
import org.apache.rocketmq.tools.command.topic.*;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class MQAdminStartup {
    protected static List<SubCommand> subCommandList = new ArrayList<SubCommand>();

    private static String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    public static void main(String[] args) {
        main0(args, null);
    }

    public static void main0(String[] args, RPCHook rpcHook) {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));

        //PackageConflictDetect.detectFastjson();

        initCommand();

        try {
            initLogback();
            switch (args.length) {
                case 0:
                    printHelp();
                    break;
                case 2:
                    if (args[0].equals("help")) {
                        SubCommand cmd = findSubCommand(args[1]);
                        if (cmd != null) {
                            Options options = ServerUtil.buildCommandlineOptions(new Options());
                            options = cmd.buildCommandlineOptions(options);
                            if (options != null) {
                                ServerUtil.printCommandLineHelp("mqadmin " + cmd.commandName(), options);
                            }
                        } else {
                            System.out.printf("The sub command %s not exist.%n", args[1]);
                        }
                        break;
                    }
                case 1:
                default:
                    SubCommand cmd = findSubCommand(args[0]);
                    if (cmd != null) {
                        String[] subargs = parseSubArgs(args);

                        Options options = ServerUtil.buildCommandlineOptions(new Options());
                        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), subargs, cmd.buildCommandlineOptions(options), new PosixParser());
                        if (null == commandLine) {
                            return;
                        }

                        if (commandLine.hasOption('n')) {
                            String namesrvAddr = commandLine.getOptionValue('n');
                            System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);
                        }

                        cmd.execute(commandLine, options, AclUtils.getAclRPCHook(rocketmqHome + MixAll.ACL_CONF_TOOLS_FILE));
                    } else {
                        System.out.printf("The sub command %s not exist.%n", args[0]);
                    }
                    break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void initCommand() {
        initCommand(new UpdateTopicSubCommand());
        initCommand(new DeleteTopicSubCommand());
        initCommand(new UpdateSubGroupSubCommand());
        initCommand(new DeleteSubscriptionGroupCommand());
        initCommand(new UpdateBrokerConfigSubCommand());
        initCommand(new UpdateTopicPermSubCommand());

        initCommand(new TopicRouteSubCommand());
        initCommand(new TopicStatusSubCommand());
        initCommand(new TopicClusterSubCommand());

        initCommand(new BrokerStatusSubCommand());
        initCommand(new QueryMsgByIdSubCommand());
        initCommand(new QueryMsgByKeySubCommand());
        initCommand(new QueryMsgByUniqueKeySubCommand());
        initCommand(new QueryMsgByOffsetSubCommand());
        initCommand(new QueryMsgTraceByIdSubCommand());

        initCommand(new PrintMessageSubCommand());
        initCommand(new PrintMessageByQueueCommand());
        initCommand(new SendMsgStatusCommand());
        initCommand(new BrokerConsumeStatsSubCommad());

        initCommand(new ProducerConnectionSubCommand());
        initCommand(new ConsumerConnectionSubCommand());
        initCommand(new ConsumerProgressSubCommand());
        initCommand(new ConsumerStatusSubCommand());
        initCommand(new CloneGroupOffsetCommand());

        initCommand(new ClusterListSubCommand());
        initCommand(new TopicListSubCommand());

        initCommand(new UpdateKvConfigCommand());
        initCommand(new DeleteKvConfigCommand());

        initCommand(new WipeWritePermSubCommand());
        initCommand(new ResetOffsetByTimeCommand());

        initCommand(new UpdateOrderConfCommand());
        initCommand(new CleanExpiredCQSubCommand());
        initCommand(new CleanUnusedTopicCommand());

        initCommand(new StartMonitoringSubCommand());
        initCommand(new StatsAllSubCommand());

        initCommand(new AllocateMQSubCommand());

        initCommand(new CheckMsgSendRTCommand());
        initCommand(new CLusterSendMsgRTCommand());

        initCommand(new GetNamesrvConfigCommand());
        initCommand(new UpdateNamesrvConfigCommand());
        initCommand(new GetBrokerConfigCommand());

        initCommand(new QueryConsumeQueueCommand());
        initCommand(new SendMessageCommand());
        initCommand(new ConsumeMessageCommand());

        //for acl command
        initCommand(new UpdateAccessConfigSubCommand());
        initCommand(new DeleteAccessConfigSubCommand());
        initCommand(new ClusterAclConfigVersionListSubCommand());
        initCommand(new UpdateGlobalWhiteAddrSubCommand());
        initCommand(new GetAccessConfigSubCommand());
    }

    private static void initLogback() throws JoranException {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(lc);
        lc.reset();
        configurator.doConfigure(rocketmqHome + "/conf/logback_tools.xml");
    }

    private static void printHelp() {
        System.out.printf("The most commonly used mqadmin commands are:%n");
        for (SubCommand cmd : subCommandList) {
            System.out.printf("   %-20s %s%n", cmd.commandName(), cmd.commandDesc());
        }

        System.out.printf("%nSee 'mqadmin help <command>' for more information on a specific command.%n");
    }

    private static SubCommand findSubCommand(final String name) {
        for (SubCommand cmd : subCommandList) {
            if (cmd.commandName().toUpperCase().equals(name.toUpperCase())) {
                return cmd;
            }
        }

        return null;
    }

    private static String[] parseSubArgs(String[] args) {
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }

    public static void initCommand(SubCommand command) {
        subCommandList.add(command);
    }
}
