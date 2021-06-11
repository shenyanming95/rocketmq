package org.apache.rocketmq.common;

import java.util.List;

public class AclConfig {

    private List<String> globalWhiteAddrs;

    private List<PlainAccessConfig> plainAccessConfigs;


    public List<String> getGlobalWhiteAddrs() {
        return globalWhiteAddrs;
    }

    public void setGlobalWhiteAddrs(List<String> globalWhiteAddrs) {
        this.globalWhiteAddrs = globalWhiteAddrs;
    }

    public List<PlainAccessConfig> getPlainAccessConfigs() {
        return plainAccessConfigs;
    }

    public void setPlainAccessConfigs(List<PlainAccessConfig> plainAccessConfigs) {
        this.plainAccessConfigs = plainAccessConfigs;
    }
}
