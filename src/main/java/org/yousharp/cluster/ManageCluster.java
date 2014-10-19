package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformationParser;

import java.util.List;

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
     * @param currentMasterNodes    master nodes in the master
     * @param newMasterNodeInfo     the new master to add
     * @param slavesOfNewMaster     slaves of the new master
     */
    public static void addNewNode(List<HostAndPort> currentMasterNodes, HostAndPort newMasterNodeInfo, List<HostAndPort> slavesOfNewMaster) {
        checkArgument(currentMasterNodes != null && !currentMasterNodes.isEmpty(), "clusterNodes cannot be empty.");
        checkNotNull(newMasterNodeInfo, "newMasterNode is invalid.");

        /** add the new node {@code newMasterNodeInfo} to the current cluster */
        HostAndPort oneNodeInfo = currentMasterNodes.iterator().next();
        Jedis oneNode = new Jedis(oneNodeInfo.getHostText(), oneNodeInfo.getPort());
        ClusterUtil.joinCluster(oneNode, newMasterNodeInfo);
        if (slavesOfNewMaster != null && !slavesOfNewMaster.isEmpty()) {
            ClusterUtil.joinCluster(oneNode, slavesOfNewMaster);
            ClusterUtil.beSlaveOfMaster(newMasterNodeInfo, slavesOfNewMaster);
        }
        List<HostAndPort> clusterNodes = Lists.newArrayList(currentMasterNodes);
        clusterNodes.add(newMasterNodeInfo);
        ClusterUtil.waitForClusterReady(clusterNodes);
        oneNode.close();

        /** migrate some slots from existing nodes to the new node, evenly to the best */
        int slotsPerNode = JedisCluster.HASHSLOTS / clusterNodes.size();
        int remainSlots = slotsPerNode;     // the slots to migrate from the last node, the remain slots.
        ClusterNodeInformationParser parser = new ClusterNodeInformationParser();
        for (int i = 0; i < currentMasterNodes.size(); i++) {
            HostAndPort hnp = currentMasterNodes.get(i);
            Jedis node = new Jedis(hnp.getHostText(), hnp.getPort());
            String nodeInfoLine = ClusterUtil.getNodeInfo(node.clusterNodes());
            node.close();
            /** todo: to implement my own version  */
            ClusterNodeInformation nodeInformation = parser.parse(nodeInfoLine, new redis.clients.jedis.HostAndPort(hnp.getHostText(), hnp.getPort()));
            List<Integer> slotsOfNode = nodeInformation.getAvailableSlots();
            int numToMigrate = (slotsOfNode.size() / JedisCluster.HASHSLOTS) * slotsPerNode;
            if (i == currentMasterNodes.size() -1 ) {
                numToMigrate = remainSlots;
            }
            remainSlots = remainSlots - numToMigrate;
            int[] slotsToMigrate = new int[numToMigrate];
            for (int j = 0; j < numToMigrate; j++) {
                slotsToMigrate[j] = slotsOfNode.get(j);
            }
            MigrateData.migrate(hnp, newMasterNodeInfo, slotsToMigrate);
        }
    }

    public static void removeNode(List<HostAndPort> currentMasterNodes, HostAndPort deleteNode) {

    }
}
