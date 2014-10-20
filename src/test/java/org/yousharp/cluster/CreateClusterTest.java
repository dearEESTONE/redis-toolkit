package org.yousharp.cluster;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ResourceBundle;

/**
 * @author: lingguo
 * @time: 2014/10/18 21:56
 */
public class CreateClusterTest extends Assert {

    private ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes = null;

    @Before
    public void loadNodes() {
        clusterNodes =  ArrayListMultimap.create();
        ResourceBundle rb = ResourceBundle.getBundle("config");
        String[] nodeArray = rb.getString("clusterNodes").split(",");
        for (String masterSlave: nodeArray) {
            String[] hostAndPorts = rb.getString(masterSlave).split(",");
            HostAndPort masterNodeInfo = HostAndPort.fromString(hostAndPorts[0]);
            if (hostAndPorts.length == 1) {
                clusterNodes.put(masterNodeInfo, null);
                continue;
            }
            for (int i = 1; i < hostAndPorts.length; i++) {
                HostAndPort slaveNodeInfo = HostAndPort.fromString(hostAndPorts[i]);
                clusterNodes.put(masterNodeInfo, slaveNodeInfo);
            }
        }
    }

    @Test
    public void testCreate() {
        assertNotNull(clusterNodes);
        CreateCluster.create(clusterNodes);
    }

}
