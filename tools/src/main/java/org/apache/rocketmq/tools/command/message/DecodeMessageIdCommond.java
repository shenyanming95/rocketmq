package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

public class DecodeMessageIdCommond implements SubCommand {
    @Override
    public String commandName() {
        return "DecodeMessageId";
    }

    @Override
    public String commandDesc() {
        return "decode unique message ID";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("i", "messageId", true, "unique message ID");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(final CommandLine commandLine, final Options options,
                        RPCHook rpcHook) throws SubCommandException {
        String messageId = commandLine.getOptionValue('i').trim();

        try {
            System.out.printf("ip=%s", MessageClientIDSetter.getIPStrFromID(messageId));
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            String date = UtilAll.formatDate(MessageClientIDSetter.getNearlyTimeFromID(messageId), UtilAll.YYYY_MM_DD_HH_MM_SS_SSS);
            System.out.printf("date=%s", date);
        } catch (Exception e) {
            throw new SubCommandException(this.getClass().getSimpleName() + " command failed", e);
        }
    }
}
