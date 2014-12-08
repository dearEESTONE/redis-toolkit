package org.yousharp.cluster;

import org.junit.Assert;
import org.junit.Test;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.HostAndPort;
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
        assertEquals(8, node.clusterNodes().split("\n").length);
        node.close();
    }

    @Test
    public void testClusterSlaves() {
        HostAndPort nodeInfo = new HostAndPort("10.7.40.49", 8000);
        Jedis node = new Jedis(nodeInfo.getHost(), nodeInfo.getPort());
        String nodeId = ClusterUtil.getNodeId(nodeInfo);
        List<String> slaves = node.clusterSlaves(nodeId);
        for (String slave: slaves) {
            System.out.println(slave);
        }
        node.close();
    }

    @Test
    public void testClusterForget() {
        Jedis node = new Jedis("10.7.40.49", 7002);

        HostAndPort forgetInfo1 = new HostAndPort("10.7.40.49", 7004);
        HostAndPort forgetInfo2 = new HostAndPort("10.7.40.49", 7005);
        String nodeId1 = ClusterUtil.getNodeId(forgetInfo1);
        String nodeId2 = ClusterUtil.getNodeId(forgetInfo2);

        node.clusterForget(nodeId1);
        node.clusterForget(nodeId2);

        node.close();

    }

    @Test
    public void testGetKeysInSlot() {
        int slot = 935;

        Jedis node = new Jedis("10.7.40.49", 7000);
        List<String> keysInSlot = node.clusterGetKeysInSlot(slot, 100);
        for (String key: keysInSlot) {
            System.out.println(key);
        }

        node.close();

    }
}
