package org.yousharp.util;

import static com.google.common.base.Preconditions.*;

import com.google.common.net.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.util.ClusterNodeInformation;
import redis.clients.util.ClusterNodeInformationParser;

import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * cluster related utility methods
 *
 * @author: lingguo
 * @time: 2014/10/18 18:20
 */
public class ClusterUtil {
    public static final String SLOT_IN_TRANSITION_IDENTIFIER = "[";
    public static final String SLOT_IMPORTING_IDENTIFIER = "--<--";
    public static final String SLOT_MIGRATING_IDENTIFIER = "-->--";
    public static final int CLUSTER_SLEEP_INTERVAL = 100;

    /**
     * wait for the cluster to be ready;
     * the cluster is ready when `cluster info` of all nodes {@code masterNodes} are ok.
     *
     * @param masterNodes   master nodes
     */
    public static void waitForClusterReady(Set<HostAndPort> masterNodes) {
        boolean clusterOk = false;
        while (!clusterOk) {
            clusterOk = true;
            for (HostAndPort hnp: masterNodes) {
                Jedis node = new Jedis(hnp.getHostText(), hnp.getPort());
                String clusterInfo = node.clusterInfo();
                String firstLine = clusterInfo.split("\r\n")[0];
                node.close();
                if (firstLine.split(":")[0].equalsIgnoreCase("cluster_state") &&
                        firstLine.split(":")[1].equalsIgnoreCase("ok")) {
                    continue;
                }
                clusterOk = false;
                break;
            }

            if (clusterOk) {
                break;
            }

            try {
                TimeUnit.MILLISECONDS.sleep(CLUSTER_SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                throw new JedisClusterException("waitForClusterReady", e);
            }
        }
    }

    /**
     * check if the node {@code node} already 'knows' the target node {@code targetNodeId},
     * which means whether they are in the same cluster;
     *
     * @param node  the current node
     * @param targetNodeId  the node id of the target node
     * @return  if 'known' return true, or return false
     */
    public static boolean isNodeKnown(Jedis node, String targetNodeId) {
        String clusterInfo = node.clusterInfo();
        for (String infoLine : clusterInfo.split("\n")) {
            if (infoLine.contains(targetNodeId)) {
                return true;
            }
        }
        return false;
    }

    /**
     * check if node {@code destNodeInfo} is in the cluster represented by node {@code srcNode};
     * if not, put it in.
     *
     * @param srcNode        the node in the cluster
     * @param destNodeInfo  the node to "meet"
     * @param timeoutMs     timeout in ms
     */
    public static void joinCluster(final Jedis srcNode, final HostAndPort destNodeInfo, final long timeoutMs) {
        Jedis destNode = new Jedis(destNodeInfo.getHostText(), destNodeInfo.getPort());
        if (!isNodeKnown(srcNode, getNodeId(destNode.clusterNodes()))) {
            srcNode.clusterMeet(destNodeInfo.getHostText(), destNodeInfo.getPort());
        }

        for (long sleepTime = 0; sleepTime <= timeoutMs; sleepTime += CLUSTER_SLEEP_INTERVAL) {
            if (isNodeKnown(srcNode, getNodeId(destNode.clusterNodes()))) {
                break;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(CLUSTER_SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                throw new JedisClusterException("joinCluster timeout.", e);
            }
        }
        destNode.close();
    }

    /**
     * put the nodes {@code destNodesInfo} to the cluster
     *
     * @param srcNode           the node in the cluster
     * @param destNodesInfo     the nodes to "meet"
     */
    public static void joinCluster(final Jedis srcNode, final List<HostAndPort> destNodesInfo, final long timeoutMs) {
        for (HostAndPort hnp: destNodesInfo) {
            joinCluster(srcNode, hnp, timeoutMs);
        }
    }

    /**
     * make nodes {@code slaves} be the slave of the {@code master} node
     *
     * @param master    the master node
     * @param slaves    slave node list
     */
    public static void beSlaveOfMaster(HostAndPort master, List<HostAndPort> slaves) {
        Jedis masterNode = new Jedis(master.getHostText(), master.getPort());
        String masterNodeId = getNodeId(masterNode.clusterNodes());
        for (HostAndPort slave: slaves) {
            Jedis slaveNode = new Jedis(slave.getHostText(), slave.getPort());
            slaveNode.clusterReplicate(masterNodeId);
            slaveNode.close();
        }
        masterNode.close();
    }

    /**
     * get node info of the current node (myself) from `cluster nodes` output info
     *
     * @param clusterNodesInfo     `cluster nodes` output
     */
    public static String getNodeInfo(String clusterNodesInfo) {
        for (String lineInfo: clusterNodesInfo.split("\n")) {
            if (lineInfo.contains("myself")) {
                return lineInfo;
            }
        }
        return "";
    }

    /**
     * get the node id of the current node
     *
     * @param clusterNodesInfo  output of `cluster nodes` of the node
     * @return  node id
     */
    public static String getNodeId(String clusterNodesInfo) {
        for (String lineInfo: clusterNodesInfo.split("\n")) {
            if (lineInfo.contains("myself")) {
                return lineInfo.split(":")[0];
            }
        }
        return "";
    }

    /**
     * allocate the slots {@code slots} to the master nodes {@code masterNodes},
     * evenly to the best.
     *
     * @param slots         the slots to allocate
     * @param masterNodes   the master nodes
     */
    public static void allocateSlotsToNodes(List<Integer> slots, List<HostAndPort> masterNodes) {
        int numOfMaster = masterNodes.size();
        int slotsPerNode = slots.size() / numOfMaster;
        int lastSlot = 0;
        for (int i = 0; i < numOfMaster; i++) {
            HostAndPort masterNodeInfo = masterNodes.get(i);
            Jedis node = new Jedis(masterNodeInfo.getHostText(), masterNodeInfo.getPort());
            /** the last node */
            if (i == numOfMaster - 1) {
                slotsPerNode = slots.size() - slotsPerNode * i;
            }
            int[] slotArray = new int[slotsPerNode];
            for (int k = lastSlot, j = 0; k < (i + 1) * slotsPerNode && j < slotsPerNode; k++, j++) {
                slotArray[j] = slots.get(k);
            }
            lastSlot = (i + 1) * slotsPerNode;
            node.clusterAddSlots(slotArray);
            node.close();
        }
    }

    /**
     * wait for the migration process done: check the output of `cluster nodes` make sure
     *  that migration is done.
     *
     * migration-in-transition:
     *  38807bd0262d99f205ebd0eb3e483cc09e927731 :7002 myself,master - 0 0 1 connected 0-5459 [5460->-38807bd0262d99f205ebd0eb3e483cc09e927731] [5461-<-e85a79cfee516d9eb1339e8f0107466307b4a50c]
     *
     * @param nodesInfo     the nodes to check
     */
    public static void waitForMigrationDone(HostAndPort nodesInfo) {
        checkNotNull(nodesInfo, "nodesInfo is null.");

        Jedis node = new Jedis(nodesInfo.getHostText(), nodesInfo.getPort());
        String[] clusterNodesInfo = node.clusterNodes().split("\n");

        boolean isOk = false;
        while (!isOk) {
            isOk = true;
            for (String infoLine: clusterNodesInfo) {
                if (infoLine.startsWith(SLOT_IN_TRANSITION_IDENTIFIER)) {
                    isOk = false;
                    break;
                }
            }
            if (isOk) {
                break;
            }
            try {
                TimeUnit.MILLISECONDS.sleep(CLUSTER_SLEEP_INTERVAL);
            } catch (InterruptedException e) {
                throw new JedisClusterException("waitForMigrationDone", e);
            }
        }
    }

    /**
     * get slots information of the node, especially the serving slots.
     *
     * @param node      the node
     * @param current   the node info
     * @return           the slots info of the node
     */
    public static ClusterNodeInformation getNodeSlotsInfo(Jedis node, HostAndPort current) {
        checkNotNull(node, "node is null.");
        checkNotNull(current, "current is null.");

        ClusterNodeInformationParser parser = new ClusterNodeInformationParser();
        String nodeInfoLine = getNodeInfo(node.clusterNodes());
        ClusterNodeInformation nodeInformation = parser.parse(nodeInfoLine, new redis.clients.jedis.HostAndPort(current.getHostText(), current.getPort()));
        return nodeInformation;
    }

}
