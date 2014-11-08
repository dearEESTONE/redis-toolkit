package org.yousharp.cluster;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Test;
import redis.clients.jedis.HostAndPort;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: lingguo
 * @time: 2014/11/7 23:11
 */
public class MultiTest extends Assert {

    @Test
    public void testMSet() {
        Set<HostAndPort> nodes = new HashSet<>();
        nodes.add(new HostAndPort("10.7.40.49", 8000));
        nodes.add(new HostAndPort("10.7.40.49", 8002));
        Multi multi = new Multi(nodes, new GenericObjectPoolConfig());
        Map<String, String> keyValues = new LinkedHashMap<>();
        keyValues.put("key5", "value5");
        keyValues.put("key6", "value6");
        keyValues.put("key7", "value7");
        keyValues.put("key8", "value8");
        Map<String, String> response = multi.mset(keyValues);
        for (String key: response.keySet()) {
            System.out.println(key + "->" + response.get(key));
        }
    }

    @Test
    public void testMGet() {
        Set<HostAndPort> nodes = Sets.newHashSet(new HostAndPort("10.7.40.49", 8000), new HostAndPort("10.7.40.49", 8002));
        List<String> keys = Lists.newArrayList("key5", "key6", "key7", "key8");
        Multi multi = new Multi(nodes, new GenericObjectPoolConfig());
        Map<String, String> response = multi.mget(keys);
        for (String key: response.keySet()) {
            System.out.println(key + "->" + response.get(key));
        }
    }
}
