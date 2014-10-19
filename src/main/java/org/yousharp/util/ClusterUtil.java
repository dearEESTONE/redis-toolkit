package org.yousharp.util;

import com.google.common.net.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * cluster相关的util方法
 *
 * @author: lingguo
 * @time: 2014/10/18 18:20
 */
public class ClusterUtil {

    /**
     * wait for the cluster to be ready;
     * the cluster is ready when `cluster info` of all nodes {@code masterNodes} are ok.
     *
     * @param masterNodes   master nodes
     */
    public static void waitForClusterReady(List<HostAndPort> masterNodes) {
        boolean clusterOk = false;
        while (!clusterOk) {
            clusterOk = true;
            for (HostAndPort hnp: masterNodes) {
                Jedis node = new Jedis(hnp.getHostText(), hnp.getPort());
                String clusterInfo = node.clusterInfo();
                String firstLine = clusterInfo.split("\n")[0];
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
                TimeUnit.MILLISECONDS.sleep(30);
            } catch (InterruptedException e) {
                throw new JedisException("cluster not ready.", e);
            }
        }
    }

    /**
     * check if the node {@code node} already 'knows' the target node {@code targetNodeId},
     * which means whether they are in the same cluster;
     *
     * @param node  the current node
     * @param targetNodeId  the node id of the target node
     * @param timeoutMs     timeout in ms
     * @return  if 'known' return true, or return false
     */
    public static boolean isNodeKnown(Jedis node, String targetNodeId, long timeoutMs) {
        String clusterInfo = node.clusterInfo();
        long sleepInterval = 100;
        for (long sleepTime = 0; sleepTime <= timeoutMs; sleepTime += sleepInterval) {
            for (String infoLine : clusterInfo.split("\n")) {
                if (infoLine.contains(targetNodeId)) {
                    return true;
                }
            }
            try {
                TimeUnit.MILLISECONDS.sleep(sleepInterval);
            } catch (InterruptedException e) {
                throw new JedisException("thread interrupted.", e);
            }
        }
        return false;
    }

    /**
     * check if node {@code destNodeInfo} is in the cluster represented by node {@code srcNode};
     * if not, put it in.
     *
     * @param srcNode       the node in the cluster
     * @param destNodeInfo  the node to "meet"
     */
    public static void joinCluster(Jedis srcNode, HostAndPort destNodeInfo) {
        Jedis destNode = new Jedis(destNodeInfo.getHostText(), destNodeInfo.getPort());
        if (!isNodeKnown(srcNode, getNodeId(destNode.clusterInfo()), 200)) {
            srcNode.clusterMeet(destNodeInfo.getHostText(), destNodeInfo.getPort());
        }
        destNode.close();
    }

    /**
     * put the nodes {@code destNodesInfo} to the cluster
     *
     * @param srcNode           the node in the cluster
     * @param destNodesInfo     the nodes to "meet"
     */
    public static void joinCluster(Jedis srcNode, List<HostAndPort> destNodesInfo) {
        for (HostAndPort hnp: destNodesInfo) {
            joinCluster(srcNode, hnp);
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

}
