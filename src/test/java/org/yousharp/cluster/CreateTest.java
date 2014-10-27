package org.yousharp.cluster;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

import java.util.ResourceBundle;

/**
 * @author: lingguo
 * @time: 2014/10/18 21:56
 */
public class CreateTest extends Assert {

    @Test
    public void testCreateFromConfig() {
        ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes = ArrayListMultimap.create();
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
        Create.create(clusterNodes);
    }

    @Test
    public void testCreateFromList() {
        /* 构建一个3主3从的集群 */
        ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes = ArrayListMultimap.create();
        clusterNodes.put(HostAndPort.fromString("127.0.0.1:7000"), HostAndPort.fromString("127.0.0.1:7001"));
        clusterNodes.put(HostAndPort.fromString("127.0.0.1:7002"), HostAndPort.fromString("127.0.0.1:7003"));
        clusterNodes.put(HostAndPort.fromString("127.0.0.1:7004"), HostAndPort.fromString("127.0.0.1:7005"));
        Create.create(clusterNodes);
    }

}
