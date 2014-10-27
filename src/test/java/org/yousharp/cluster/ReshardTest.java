package org.yousharp.cluster;

import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author: lingguo
 * @time: 2014/10/20 14:42
 */
public class ReshardTest extends Assert {

    @Test
    public void testMigrate() {
        /** 从节点7000迁移1000个slot到节点7004 **/
        HostAndPort srcNodeInfo = HostAndPort.fromString("10.7.40.49:7000");
        HostAndPort destNodeInfo = HostAndPort.fromString("10.7.40.49:7004");
        Reshard.migrate(srcNodeInfo, destNodeInfo, 100);
    }

    @Test
    public void testMigrateSlot() {
        /** 将slot 9189从节点7002迁移到节点7006 **/
        HostAndPort srcNodeInfo = HostAndPort.fromString("10.7.40.49:7002");
        HostAndPort destNodeInfo = HostAndPort.fromString("10.7.40.49:7006");
        Reshard.migrateSlots(srcNodeInfo, destNodeInfo, 9189);
    }
}
