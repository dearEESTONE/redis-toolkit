package org.yousharp.cluster;

import com.google.common.collect.ArrayListMultimap;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

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
            String[] masterNodeStr = hostAndPorts[0].split(":");
            HostAndPort masterNodeInfo = new HostAndPort(masterNodeStr[0], Integer.valueOf(masterNodeStr[1]));
            if (hostAndPorts.length == 1) {
                clusterNodes.put(masterNodeInfo, null);
                continue;
            }
            for (int i = 1; i < hostAndPorts.length; i++) {
//                HostAndPort slaveNodeInfo = HostAndPort.fromString(hostAndPorts[i]);
                String[] slaveNodeStr = hostAndPorts[i].split(":");
                HostAndPort slaveNodeInfo = new HostAndPort(slaveNodeStr[0], Integer.valueOf(slaveNodeStr[1]));
                clusterNodes.put(masterNodeInfo, slaveNodeInfo);
            }
        }
        Create.create(clusterNodes);
    }

    @Test
    public void testCreateFromList() {
        /* 构建一个3主3从的集群 */
        String host = "127.0.0.1";
        ArrayListMultimap<HostAndPort, HostAndPort> clusterNodes = ArrayListMultimap.create();
        clusterNodes.put(new HostAndPort(host, 7000), new HostAndPort(host, 7001));
        clusterNodes.put(new HostAndPort(host, 7002), new HostAndPort(host, 7003));
        clusterNodes.put(new HostAndPort(host, 7004), new HostAndPort(host, 7005));
        Create.create(clusterNodes);
    }

}
