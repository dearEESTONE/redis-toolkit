package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Sets;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.util.ClusterNodeInformation;

import java.util.List;

/**
 * play with cluster
 *  - add a new node (master/slave) to the cluster;
 *  - remove a node (master/slave) from the cluster;
 *
 * @author: lingguo
 * @time: 2014/10/19 9:10
 */
public class Manage {

    /**
     * add a new node to the cluster:
     *  1. if the node to add is master, add it to the cluster and migrate slots;
     *  2. if the node to add is slave of a master in the cluster, first `cluster meet`
     *   to join the cluster, then `cluster replicate` to be slave of the master.
     *
     * @param clusterNodeInfo   any node in the cluster
     * @param nodeToAdd         the node to add
     * @param masterToReplicate     if the node to add is slave, this is it's master in the cluster, or this is null.
     */
    public static void addNewNode(final HostAndPort clusterNodeInfo, final HostAndPort nodeToAdd, final HostAndPort masterToReplicate) {
        checkNotNull(clusterNodeInfo, "clusterNodeInfo is null.");
        checkNotNull(nodeToAdd, "nodeToAdd is invalid.");

        List<HostAndPort> oldMasterNodes = ClusterUtil.getMasterNodesOfCluster(clusterNodeInfo);

        // the node to add is master
        if (masterToReplicate == null) {
            ClusterUtil.joinCluster(clusterNodeInfo, nodeToAdd);
            /** migrate some slots from existing masters to the new master, evenly to the best */
            List<HostAndPort> newMasterNodes = ClusterUtil.getMasterNodesOfCluster(clusterNodeInfo);
            int slotsPerMaster = JedisCluster.HASHSLOTS / newMasterNodes.size();
            int remainSlots = slotsPerMaster;     // the slots to migrate from the last node, the remain slots.
            for (int i = 0; i < oldMasterNodes.size(); i++) {
                HostAndPort nodeInfo = oldMasterNodes.get(i);
                ClusterNodeInformation slotsInfo = ClusterUtil.getNodeSlotsInfo(nodeInfo);
                List<Integer> slotsOfNode = slotsInfo.getAvailableSlots();
                if (slotsOfNode.isEmpty()) {
                    continue;       // no slots to migrate, skip
                }
                /** migrate proportionally: (slotsOfNode.size() / JedisCluster.HASHSLOTS) * slotsPerMaster */
                int numToMigrate = (slotsOfNode.size() * slotsPerMaster) / JedisCluster.HASHSLOTS;
                if (i == oldMasterNodes.size() - 1) {
                    numToMigrate = remainSlots;
                }
                if (numToMigrate == 0) {
                    continue;       // no need to migrate
                }
                remainSlots = remainSlots - numToMigrate;
                Reshard.migrate(nodeInfo, nodeToAdd, numToMigrate);
            }
        } else {        // the node to add is slave, replicate to masterToReplicate in the cluster
            ClusterUtil.joinCluster(clusterNodeInfo, nodeToAdd);
            Jedis node = new Jedis(nodeToAdd.getHost(), nodeToAdd.getPort());
            String masterNodeId = ClusterUtil.getNodeId(masterToReplicate);
            node.clusterReplicate(masterNodeId);
            node.close();
        }

    }

    /**
     * remove a node from the cluster:
     *  1. if the node is slave, use `cluster forget` for every node in the cluster to forget the node;
     *  2. if the node is master:
     *   2.1 if the node doesn't serves slots, remove the node and all it's slaves from the cluster;
     *   2.2 if the node serves slots, reshard the slot to other masters and remove the node and all
     *    it's slaves from the cluster;
     *
     * @param oneNodeInfo           any node of the cluster
     * @param nodeToDelete      the node to delete
     */
    public static void removeNode(final HostAndPort oneNodeInfo, final HostAndPort nodeToDelete) {
        Jedis deleteNode = new Jedis(nodeToDelete.getHost(), nodeToDelete.getPort());
        String deleteNodeId = ClusterUtil.getNodeId(nodeToDelete);
        List<HostAndPort> allNodesOfCluster = ClusterUtil.getAllNodesOfCluster(oneNodeInfo);

        // check if the node to delete is a master
        boolean isMaster = false;
        if (ClusterUtil.getNodeInfo(nodeToDelete).contains("master")) {
            isMaster = true;
        }

        // the node to delete is slave
        if (!isMaster) {
            for (HostAndPort nodeInfo: allNodesOfCluster) {
                // a node cannot `forget` itself
                if (!nodeInfo.equals(nodeToDelete)) {
                    Jedis node = new Jedis(nodeInfo.getHost(), nodeInfo.getPort());
                    node.clusterForget(deleteNodeId);
                    node.close();
                }
            }
            allNodesOfCluster.remove(nodeToDelete);
            ClusterUtil.waitForClusterReady(Sets.newHashSet(allNodesOfCluster));
            return;
        }

        // the node to delete is master
        ClusterNodeInformation nodeSlotsInfo = ClusterUtil.getNodeSlotsInfo(nodeToDelete);
        List<Integer> availableSlots = nodeSlotsInfo.getAvailableSlots();
        // no slots on the master
        if (!availableSlots.isEmpty()) {
            List<HostAndPort> masterOfCluster = ClusterUtil.getMasterNodesOfCluster(oneNodeInfo);
            masterOfCluster.remove(nodeToDelete);
            int slotsToEachMaster = availableSlots.size() / masterOfCluster.size();
            int remainSlots = availableSlots.size() - slotsToEachMaster * masterOfCluster.size();

            for (int i = 0; i < masterOfCluster.size(); i++) {
                Reshard.migrate(nodeToDelete, masterOfCluster.get(i), slotsToEachMaster);
                if (i == masterOfCluster.size() - 1 && remainSlots > 0) {
                    Reshard.migrate(nodeToDelete, masterOfCluster.get(i), remainSlots);
                }
            }
            ClusterUtil.waitForMigrationDone(nodeToDelete);
        }

        List<String> slavesOfMaster = deleteNode.clusterSlaves(deleteNodeId);
        // remove the master and it's slaves from the cluster
        allNodesOfCluster.remove(nodeToDelete);
        for (String slaveStr: slavesOfMaster) {
            String[] hostAndPort = slaveStr.split(" ")[1].split(":");
            HostAndPort slaveInfo = new HostAndPort(hostAndPort[0], Integer.valueOf(hostAndPort[1]));
            allNodesOfCluster.remove(slaveInfo);
        }

        // every node of the cluster `forget` the master and it's slaves
        for (HostAndPort nodeInfo: allNodesOfCluster) {
            Jedis node = new Jedis(nodeInfo.getHost(), nodeInfo.getPort());
            node.clusterForget(deleteNodeId);
            for (String slaveStr: slavesOfMaster) {
                node.clusterForget(slaveStr.split(" ")[0]);
            }
            node.close();
        }
        deleteNode.close();
    }
}
