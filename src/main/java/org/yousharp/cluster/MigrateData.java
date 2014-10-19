package org.yousharp.cluster;

import static com.google.common.base.Preconditions.*;

import com.google.common.net.HostAndPort;
import org.yousharp.util.ClusterUtil;
import redis.clients.jedis.Jedis;

/**
 * Data migration: migrate slots from one node to another;
 *
 * @author: lingguo
 * @time: 2014/10/19 0:38
 */
public class MigrateData {

    /**
     * migrate slots {@code slots} from node {@code srcNodeInfo} to node {@code destNodeInfo};
     *
     * @param srcNodeInfo   source node that migrates from
     * @param destNodeInfo  dest node that migrates to
     * @param slots     the slots to migrate
     */
    public static void migrate(HostAndPort srcNodeInfo, HostAndPort destNodeInfo, int... slots) {
        checkNotNull(srcNodeInfo, "srcNodeInfo cannot be null.");
        checkNotNull(destNodeInfo, "destNodeInfo cannot be null.");
        checkArgument(slots.length > 0, "slots size cannot be 0.");

        Jedis srcNode = new Jedis(srcNodeInfo.getHostText(), srcNodeInfo.getPort());
        String srcNodeId = ClusterUtil.getNodeId(srcNode.clusterNodes());
        Jedis destNode = new Jedis(destNodeInfo.getHostText(), destNodeInfo.getPort());
        String destNodeId = ClusterUtil.getNodeId(destNode.clusterNodes());

        for (int slot: slots) {
            srcNode.clusterSetSlotMigrating(slot, destNodeId);
            destNode.clusterSetSlotImporting(slot, srcNodeId);

            srcNode.clusterSetSlotNode(slot, destNodeId);
            destNode.clusterSetSlotNode(slot, destNodeId);
        }

    }
}
