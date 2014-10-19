package org.yousharp.cluster;

import org.junit.Assert;
import org.junit.Test;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author: lingguo
 * @time: 2014/10/19 15:55
 */
public class ClusterMiscTest extends Assert {

    @Test
    public void testClusterNodes() {
        Jedis node = new Jedis("10.7.40.49", 7000);
        assertEquals(1, node.clusterNodes().split("\n").length);
        node.close();
    }

    @Test
    public void testClusterSlaves() {
        Jedis node = new Jedis("192.168.106.210", 6481);
        String nodeId = ClusterUtil.getNodeId(node.clusterNodes());
        List<String> slaves = node.clusterSlaves(nodeId);
        for (String slave: slaves) {
            System.out.println("slave: " + slave);
        }
    }
}
