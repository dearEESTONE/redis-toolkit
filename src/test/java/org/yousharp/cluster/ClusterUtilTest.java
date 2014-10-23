package org.yousharp.cluster;

import com.google.common.net.HostAndPort;
import org.junit.Test;
import org.yousharp.util.ClusterUtil;

import java.util.List;

/**
 * @author: lingguo
 * @time: 2014/10/23 15:12
 */
public class ClusterUtilTest {

    @Test
    public void testGetAllNodesOfCluster() {
        HostAndPort nodeInfo = HostAndPort.fromString("10.7.40.49:8000");
        List<HostAndPort> nodeList = ClusterUtil.getAllNodesOfCluster(nodeInfo);
        for (HostAndPort hostAndPort: nodeList) {
            System.out.println(hostAndPort);
        }
    }
}
