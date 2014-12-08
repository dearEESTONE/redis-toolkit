package org.yousharp.cluster;

import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

/**
 * @author: lingguo
 * @time: 2014/10/20 18:15
 */
public class ManageTest extends Assert {

    private final String HOST = "10.7.40.49";

    @Test
    public void testAddMater() {
        /** 向集群中增加一个master节点7006 **/
        HostAndPort clusterNodeInfo = new HostAndPort(HOST, 7000);
        HostAndPort newMaster = new HostAndPort(HOST, 7006);
        Manage.addNewNode(clusterNodeInfo, newMaster, null);
    }

    @Test
    public void testAddSlave() {
        /** 给集群中增加一个slave节点8001，作为master节点7002的slave **/
        HostAndPort clusterNodeInfo = new HostAndPort(HOST, 7000);
        HostAndPort master = new HostAndPort(HOST, 7002);
        HostAndPort newSlave = new HostAndPort(HOST, 8001);
        Manage.addNewNode(clusterNodeInfo, newSlave, master);
    }

    @Test
    public void testRemoveSlave() {
        /* 从集群中删除一个节点，删除时，集群会判断待删除的节点是master还是slave */
        HostAndPort oneNode = new HostAndPort(HOST, 7000);
        HostAndPort nodeToDelete = new HostAndPort(HOST, 7007);
        Manage.removeNode(oneNode, nodeToDelete);
    }

    @Test
    public void testRemoveMaster() {
        /* 从集群中删除一个节点，删除时，集群会判断待删除的节点是master还是slave */
        HostAndPort oneNode = new HostAndPort(HOST, 7000);
        HostAndPort nodeToDelete = new HostAndPort(HOST, 7006);
        Manage.removeNode(oneNode, nodeToDelete);
    }
}
