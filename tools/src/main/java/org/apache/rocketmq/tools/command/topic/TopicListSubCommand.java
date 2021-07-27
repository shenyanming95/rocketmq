package org.apache.rocketmq.tools.command.topic;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class TopicListSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicList";
    }

    @Override
    public String commandDesc() {
        return "Fetch all topic list from name server";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterModel", false, "clusterModel");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) throws SubCommandException {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();
            if (commandLine.hasOption('c')) {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

                System.out.printf("%-20s  %-48s  %-48s%n", "#Cluster Name", "#Topic", "#Consumer Group");

                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX) || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                        continue;
                    }

                    String clusterName = "";
                    GroupList groupList = new GroupList();

                    try {
                        clusterName = this.findTopicBelongToWhichCluster(topic, clusterInfo, defaultMQAdminExt);
                        groupList = defaultMQAdminExt.queryTopicConsumeByWho(topic);
                    } catch (Exception e) {
                    }

                    if (null == groupList || groupList.getGroupList().isEmpty()) {
                        groupList = new GroupList();
                        groupList.getGroupList().add("");
                    }

                    for (String group : groupList.getGroupList()) {
                        System.out.printf("%-20s  %-48s  %-48s%n", UtilAll.frontStringAtLeast(clusterName, 20), UtilAll.frontStringAtLeast(topic, 48), UtilAll.frontStringAtLeast(group, 48));
                    }
                }
            } else {
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    System.out.printf("%s%n", topic);
                }
            }
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private String findTopicBelongToWhichCluster(final String topic, final ClusterInfo clusterInfo, final DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQClientException, InterruptedException {
        TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

        BrokerData brokerData = topicRouteData.getBrokerDatas().get(0);

        String brokerName = brokerData.getBrokerName();

        Iterator<Entry<String, Set<String>>> it = clusterInfo.getClusterAddrTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<String>> next = it.next();
            if (next.getValue().contains(brokerName)) {
                return next.getKey();
            }
        }
        return null;
    }
}
