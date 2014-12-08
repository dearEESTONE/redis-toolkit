package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * create cluster using the specified master/slave instance;
 *
 * @author: lingguo
 * @time: 2014/10/18 11:21
 */
public class Create {

    /**
     * create a redis cluster
     *
     * @param clusterNodes  nodes of the cluster, key/value is master/slave, one master can have multiple slaves
     */
    public static void create(final ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes) {
        checkArgument(clusterNodes != null && clusterNodes.size() > 0, "invalid clusterNodes.");

        /**
         * 1. use `cluster meet` to build a cluster with the nodes
         */
        Jedis firstNode = null;
        for (Map.Entry<HostAndPort, HostAndPort> pair: clusterNodes.entries()) {
            HostAndPort masterNodeInfo = pair.getKey();
            HostAndPort slaveNodeInfo = pair.getValue();

            if (firstNode == null) {
                firstNode = new Jedis(masterNodeInfo.getHost(), masterNodeInfo.getPort());
                ClusterUtil.joinCluster(masterNodeInfo, slaveNodeInfo);
                continue;
            }
            ClusterUtil.joinCluster(masterNodeInfo, masterNodeInfo);
            ClusterUtil.joinCluster(masterNodeInfo, slaveNodeInfo);
        }
        if (firstNode != null) {
            firstNode.close();
        }

        /**
         * 2. use `cluster replicate` to build master/slave structure
         */
        for (HostAndPort master: clusterNodes.keySet()) {
            List<HostAndPort> slaveList = clusterNodes.get(master);
            ClusterUtil.beSlaveOfMaster(master, slaveList);
        }

        /**
         * 3. use `cluster addslots` to allocate the 16384 slots to all master nodes,
         * the last node may have a little more or less slots.
         */
        List<Integer> slots = new ArrayList<>();
        for (int i = 0; i < JedisCluster.HASHSLOTS; i++) {
            slots.add(i);
        }
        ClusterUtil.allocateSlotsToNodes(slots, Lists.newArrayList(clusterNodes.keys()));

        /**
         * 4. use `cluster info` to make sure that all nodes are ok;
         * wait for the cluster to be ready.
         */
        ClusterUtil.waitForClusterReady(clusterNodes.keySet());
    }
}
