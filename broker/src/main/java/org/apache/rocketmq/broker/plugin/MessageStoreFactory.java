package org.apache.rocketmq.broker.plugin;

import org.apache.rocketmq.store.MessageStore;

import java.io.IOException;
import java.lang.reflect.Constructor;

public final class MessageStoreFactory {
    public final static MessageStore build(MessageStorePluginContext context, MessageStore messageStore) throws IOException {
        String plugin = context.getBrokerConfig().getMessageStorePlugIn();
        if (plugin != null && plugin.trim().length() != 0) {
            String[] pluginClasses = plugin.split(",");
            for (int i = pluginClasses.length - 1; i >= 0; --i) {
                String pluginClass = pluginClasses[i];
                try {
                    @SuppressWarnings("unchecked") Class<AbstractPluginMessageStore> clazz = (Class<AbstractPluginMessageStore>) Class.forName(pluginClass);
                    Constructor<AbstractPluginMessageStore> construct = clazz.getConstructor(MessageStorePluginContext.class, MessageStore.class);
                    messageStore = construct.newInstance(context, messageStore);
                } catch (Throwable e) {
                    throw new RuntimeException(String.format("Initialize plugin's class %s not found!", pluginClass), e);
                }
            }
        }
        return messageStore;
    }
}
