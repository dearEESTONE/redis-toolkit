package org.yousharp.cluster;

import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: lingguo
 * @time: 2014/10/20 18:15
 */
public class ManageTest extends Assert {

    private final String HOST = "10.7.40.49";

    @Test
    public void testAddMater() {
        HostAndPort clusterNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort newMaster = HostAndPort.fromString("10.7.40.49:7006");

        Manage.addNewNode(clusterNodeInfo, newMaster, null);
    }

    @Test
    public void testRemoveSlave() {
        HostAndPort oneNode = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort nodeToDelete = HostAndPort.fromString("10.7.40.49:7007");
        Manage.removeNode(oneNode, nodeToDelete);
    }

    @Test
    public void testRemoveMaster() {
        HostAndPort oneNode = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort nodeToDelete = HostAndPort.fromString("10.7.40.49:7006");
        Manage.removeNode(oneNode, nodeToDelete);
    }
}
