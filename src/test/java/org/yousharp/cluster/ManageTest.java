package org.yousharp.cluster;

import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: lingguo
 * @time: 2014/10/20 18:15
 */
public class ManageTest extends Assert {

    @Test
    public void testAddMater() {
        /** 向集群中增加一个master节点7006 **/
        HostAndPort clusterNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort newMaster = HostAndPort.fromString("10.7.40.49:7006");
        Manage.addNewNode(clusterNodeInfo, newMaster, null);
    }

    @Test
    public void testAddSlave() {
        /** 给集群中增加一个slave节点8001，作为master节点7002的slave **/
        HostAndPort clusterNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort master = HostAndPort.fromString("10.7.40.49:7002");
        HostAndPort newSlave = HostAndPort.fromString("10.7.40.49:8001");
        Manage.addNewNode(clusterNodeInfo, newSlave, master);
    }

    @Test
    public void testRemoveSlave() {
        /* 从集群中删除一个节点，删除时，集群会判断待删除的节点是master还是slave */
        HostAndPort oneNode = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort nodeToDelete = HostAndPort.fromString("10.7.40.49:7007");
        Manage.removeNode(oneNode, nodeToDelete);
    }

    @Test
    public void testRemoveMaster() {
        /* 从集群中删除一个节点，删除时，集群会判断待删除的节点是master还是slave */
        HostAndPort oneNode = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort nodeToDelete = HostAndPort.fromString("10.7.40.49:7006");
        Manage.removeNode(oneNode, nodeToDelete);
    }
}
