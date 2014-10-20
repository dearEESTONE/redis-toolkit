package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.util.ClusterNodeInformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * manage capacity of the cluster:
 *  - add a new master node and migrate slots to it;
 *  - delete an existing master node and recycle the slots on it.
 *
 * @author: lingguo
 * @time: 2014/10/19 9:10
 */
public class ManageCluster {

    /**
     * add a new node (with or without slaves) to the master
     *  1. join the new node to the current master;
     *  2. migrate slots to the new node.
     *
     * @param oneMasterNode    (any) one master node in the cluster
     * @param newMasterNodeInfo     the new master to add
     * @param slavesOfNewMaster     slaves of the new master
     */
    public static void addNewNode(final Jedis oneMasterNode, final HostAndPort newMasterNodeInfo, List<HostAndPort> slavesOfNewMaster) {
        checkNotNull(oneMasterNode, "node is null.");
        checkNotNull(newMasterNodeInfo, "newMasterNode is invalid.");

        String[] clusterNodesInfoArray = oneMasterNode.clusterNodes().split("\n");
        List<HostAndPort> masterNodesList = new ArrayList<>();
        for (String lineInfo: clusterNodesInfoArray) {
            if (lineInfo.contains("master")) {
                String address = lineInfo.split(" ")[1];
                masterNodesList.add(HostAndPort.fromString(address));
            }
        }

        if (masterNodesList.contains(newMasterNodeInfo)) {
            Jedis newNode = new Jedis(newMasterNodeInfo.getHostText(), newMasterNodeInfo.getPort());
            ClusterNodeInformation slotsInfo = ClusterUtil.getNodeSlotsInfo(newNode, newMasterNodeInfo);
            if (slotsInfo.getAvailableSlots().size() > 0) {
                return;     // the {newMasterNodeInfo} is not a new node, return;
            }
            /** the {newMasterNodeInfo} is recognized in the cluster, but does not serve any slots.  */
            masterNodesList.remove(newMasterNodeInfo);
        }

        /** add the new node {@code newMasterNodeInfo} to the current cluster */
        ClusterUtil.joinCluster(oneMasterNode, newMasterNodeInfo, ClusterUtil.CLUSTER_SLEEP_INTERVAL);
        if (slavesOfNewMaster != null && !slavesOfNewMaster.isEmpty()) {
            ClusterUtil.joinCluster(oneMasterNode, slavesOfNewMaster, ClusterUtil.CLUSTER_SLEEP_INTERVAL);
            ClusterUtil.beSlaveOfMaster(newMasterNodeInfo, slavesOfNewMaster);
        }
        Set<HostAndPort> clusterNodes = Sets.newHashSet(masterNodesList);
        clusterNodes.add(newMasterNodeInfo);
        ClusterUtil.waitForClusterReady(clusterNodes);
        oneMasterNode.close();

        /** migrate some slots from existing nodes to the new node, evenly to the best */
        int slotsPerNode = JedisCluster.HASHSLOTS / clusterNodes.size();
        int remainSlots = slotsPerNode;     // the slots to migrate from the last node, the remain slots.
        for (int i = 0; i < masterNodesList.size(); i++) {
            HostAndPort hnp = masterNodesList.get(i);
            Jedis node = new Jedis(hnp.getHostText(), hnp.getPort());
            ClusterNodeInformation slotsInfo = ClusterUtil.getNodeSlotsInfo(node, hnp);
            List<Integer> slotsOfNode = slotsInfo.getAvailableSlots();
            if (slotsOfNode.isEmpty()) {
                continue;       // no slots to migrate, skip
            }
            /** migrate proportionally: (slotsOfNode.size() / JedisCluster.HASHSLOTS) / slotsPerNode */
            int numToMigrate = (slotsOfNode.size() * slotsPerNode) / JedisCluster.HASHSLOTS;
            if (i == masterNodesList.size() -1 ) {
                numToMigrate = remainSlots;
            }
            if (numToMigrate == 0) {
                continue;       // no need to migrate
            }
            remainSlots = remainSlots - numToMigrate;
            int[] slotsToMigrate = new int[numToMigrate];
            for (int j = 0; j < numToMigrate; j++) {
                slotsToMigrate[j] = slotsOfNode.get(j);
            }
            MigrateData.migrateSlots(hnp, newMasterNodeInfo, slotsToMigrate);
        }
    }

    public static void removeNode(List<HostAndPort> currentMasterNodes, HostAndPort deleteNode) {

    }
}
