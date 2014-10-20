package org.yousharp.cluster;

import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author: lingguo
 * @time: 2014/10/20 18:15
 */
public class ManageClusterTest extends Assert {

    @Test
    public void testAddNewNode() {
        HostAndPort firstNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
        Jedis node = new Jedis(firstNodeInfo.getHostText(), firstNodeInfo.getPort());
        HostAndPort newMasterNodeInfo = HostAndPort.fromString("10.7.40.49:7006");
        List<HostAndPort> slaveOfNewMaster = Lists.newArrayList(HostAndPort.fromString("10.7.40.49:7007"));

        ManageCluster.addNewNode(node, newMasterNodeInfo, slaveOfNewMaster);
    }
}
