package org.apache.rocketmq.acl;

import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;

public interface AccessValidator {

    /**
     * Parse to get the AccessResource(user, resource, needed permission)
     *
     * @param request
     * @param remoteAddr
     * @return Plain access resource result,include access key,signature and some other access attributes.
     */
    AccessResource parse(RemotingCommand request, String remoteAddr);

    /**
     * Validate the access resource.
     *
     * @param accessResource
     */
    void validate(AccessResource accessResource);

    /**
     * Update the access resource config
     *
     * @param plainAccessConfig
     * @return
     */
    boolean updateAccessConfig(PlainAccessConfig plainAccessConfig);

    /**
     * Delete the access resource config
     *
     * @return
     */
    boolean deleteAccessConfig(String accesskey);

    /**
     * Get the access resource config version information
     *
     * @return
     */
    String getAclConfigVersion();

    /**
     * Update globalWhiteRemoteAddresses in acl yaml config file
     *
     * @return
     */
    boolean updateGlobalWhiteAddrsConfig(List<String> globalWhiteAddrsList);

    /**
     * get broker cluster acl config information
     *
     * @return
     */
    AclConfig getAllAclConfig();
}
