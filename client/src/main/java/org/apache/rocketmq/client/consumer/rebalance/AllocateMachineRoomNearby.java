package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * An allocate strategy proxy for based on machine room nearside priority. An actual allocate strategy can be
 * specified.
 * <p>
 * If any consumer is alive in a machine room, the message queue of the broker which is deployed in the same machine
 * should only be allocated to those. Otherwise, those message queues can be shared along all consumers since there are
 * no alive consumer to monopolize them.
 */
public class AllocateMachineRoomNearby implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    private final AllocateMessageQueueStrategy allocateMessageQueueStrategy;//actual allocate strategy
    private final MachineRoomResolver machineRoomResolver;

    public AllocateMachineRoomNearby(AllocateMessageQueueStrategy allocateMessageQueueStrategy, MachineRoomResolver machineRoomResolver) throws NullPointerException {
        if (allocateMessageQueueStrategy == null) {
            throw new NullPointerException("allocateMessageQueueStrategy is null");
        }

        if (machineRoomResolver == null) {
            throw new NullPointerException("machineRoomResolver is null");
        }

        this.allocateMessageQueueStrategy = allocateMessageQueueStrategy;
        this.machineRoomResolver = machineRoomResolver;
    }

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}", consumerGroup, currentCID, cidAll);
            return result;
        }

        //group mq by machine room
        Map<String/*machine room */, List<MessageQueue>> mr2Mq = new TreeMap<String, List<MessageQueue>>();
        for (MessageQueue mq : mqAll) {
            String brokerMachineRoom = machineRoomResolver.brokerDeployIn(mq);
            if (StringUtils.isNoneEmpty(brokerMachineRoom)) {
                if (mr2Mq.get(brokerMachineRoom) == null) {
                    mr2Mq.put(brokerMachineRoom, new ArrayList<MessageQueue>());
                }
                mr2Mq.get(brokerMachineRoom).add(mq);
            } else {
                throw new IllegalArgumentException("Machine room is null for mq " + mq);
            }
        }

        //group consumer by machine room
        Map<String/*machine room */, List<String/*clientId*/>> mr2c = new TreeMap<String, List<String>>();
        for (String cid : cidAll) {
            String consumerMachineRoom = machineRoomResolver.consumerDeployIn(cid);
            if (StringUtils.isNoneEmpty(consumerMachineRoom)) {
                if (mr2c.get(consumerMachineRoom) == null) {
                    mr2c.put(consumerMachineRoom, new ArrayList<String>());
                }
                mr2c.get(consumerMachineRoom).add(cid);
            } else {
                throw new IllegalArgumentException("Machine room is null for consumer id " + cid);
            }
        }

        List<MessageQueue> allocateResults = new ArrayList<MessageQueue>();

        //1.allocate the mq that deploy in the same machine room with the current consumer
        String currentMachineRoom = machineRoomResolver.consumerDeployIn(currentCID);
        List<MessageQueue> mqInThisMachineRoom = mr2Mq.remove(currentMachineRoom);
        List<String> consumerInThisMachineRoom = mr2c.get(currentMachineRoom);
        if (mqInThisMachineRoom != null && !mqInThisMachineRoom.isEmpty()) {
            allocateResults.addAll(allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mqInThisMachineRoom, consumerInThisMachineRoom));
        }

        //2.allocate the rest mq to each machine room if there are no consumer alive in that machine room
        for (String machineRoom : mr2Mq.keySet()) {
            if (!mr2c.containsKey(machineRoom)) { // no alive consumer in the corresponding machine room, so all consumers share these queues
                allocateResults.addAll(allocateMessageQueueStrategy.allocate(consumerGroup, currentCID, mr2Mq.get(machineRoom), cidAll));
            }
        }

        return allocateResults;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM_NEARBY" + "-" + allocateMessageQueueStrategy.getName();
    }

    /**
     * A resolver object to determine which machine room do the message queues or clients are deployed in.
     * <p>
     * AllocateMachineRoomNearby will use the results to group the message queues and clients by machine room.
     * <p>
     * The result returned from the implemented method CANNOT be null.
     */
    public interface MachineRoomResolver {
        String brokerDeployIn(MessageQueue messageQueue);

        String consumerDeployIn(String clientID);
    }
}
