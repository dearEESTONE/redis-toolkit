package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.Map;
import java.util.Set;

/**
 * create cluster using the specified master/slave instance;
 *
 * @author: lingguo
 * @time: 2014/10/18 11:21
 */
public class CreateCluster {

    /**
     * create a redis cluster
     *
     * @param clusterNodes  nodes of the cluster, key/value is master/slave, one master can have multiple slaves
     */
    public static void create(final HashMultimap<HostAndPort, HostAndPort> clusterNodes) {
        checkArgument(clusterNodes != null && clusterNodes.size() > 0, "invalid clusterNodes.");

        /**
         * 1. use `cluster meet` to build a cluster with the nodes
         */
        Jedis firstNode = null;
        for (Map.Entry<HostAndPort, HostAndPort> pair: clusterNodes.entries()) {
            HostAndPort masterNodeInfo = pair.getKey();
            HostAndPort slaveNodeInfo = pair.getValue();

            if (firstNode == null) {
                firstNode = new Jedis(masterNodeInfo.getHostText(), masterNodeInfo.getPort());
                firstNode.clusterMeet(slaveNodeInfo.getHostText(), slaveNodeInfo.getPort());
                continue;
            }
            firstNode.clusterMeet(masterNodeInfo.getHostText(), masterNodeInfo.getPort());
            firstNode.clusterMeet(slaveNodeInfo.getHostText(), slaveNodeInfo.getPort());
            firstNode.close();
        }

        /**
         * 2. use `cluster replicate` to build master/slave structure
         */
        for (HostAndPort master: clusterNodes.keySet()) {
            Set<HostAndPort> slaveList = clusterNodes.get(master);
            Jedis masterNode = new Jedis(master.getHostText(), master.getPort());
            String masterNodeId = ClusterUtil.getNodeId(masterNode.clusterNodes());
            for (HostAndPort slave: slaveList) {
                if (slave == null) {
                    continue;
                }
                Jedis slaveNode = new Jedis(slave.getHostText(), slave.getPort());
                slaveNode.clusterReplicate(masterNodeId);
                slaveNode.close();
            }
            masterNode.close();
        }

        /**
         * 3.allocate the 16384 slots to all master nodes, the last node may have
         * a little more or less slots
         */
        int numOfMaster = clusterNodes.keySet().size();
        int slotsPerNode = JedisCluster.HASHSLOTS / numOfMaster;
        int nodeIndex = 1;
        int lastSlot = 0;
        for (HostAndPort hnp: clusterNodes.keySet()) {
            Jedis node = new Jedis(hnp.getHostText(), hnp.getPort());
            if (nodeIndex == numOfMaster) {
                slotsPerNode = JedisCluster.HASHSLOTS - slotsPerNode * (numOfMaster - 1);
            }
            int[] slotArray = new int[slotsPerNode];
            for (int i = lastSlot, j = 0; i < nodeIndex * slotsPerNode && j < slotsPerNode; i++, j++) {
                slotArray[j] = i;
            }
            lastSlot = nodeIndex++ * slotsPerNode;
            node.clusterAddSlots(slotArray);
            node.close();
        }

        /**
         * 4. wait for the cluster to be ready
         */
        ClusterUtil.waitForClusterReady(Lists.newArrayList(clusterNodes.keySet()));
    }
}
