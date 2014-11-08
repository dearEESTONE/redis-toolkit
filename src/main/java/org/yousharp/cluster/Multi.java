package org.yousharp.cluster;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSlotBasedConnectionHandler;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.util.JedisClusterCRC16;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * implment mset/mget... in cluster
 *
 * @author: lingguo
 * @time: 2014/11/7 20:06
 */
public class Multi {
    private CustomConnectionHandler connectionHandler;

    public Multi(Set<HostAndPort> clusterNodes, final GenericObjectPoolConfig poolConfig) {
        this.connectionHandler = new CustomConnectionHandler(clusterNodes, poolConfig);
    }

    /**
     * mset:
     * @param keyValues
     * @return
     */
    public Map<String, String> mset(final Map<String, String> keyValues) {
        List<String> keys = new ArrayList<>(keyValues.keySet());
        Map<JedisPool, List<String>> poolListMap = getPoolKeysMap(keys);
        Map<String, String> resultResponse = new LinkedHashMap<>();

        if (poolListMap == null) {
            return null;
        }
        for (JedisPool pool: poolListMap.keySet()) {
            Jedis jedis = null;
            boolean isBroken = false;
            try {
                List<String> keyList = poolListMap.get(pool);
                if (keyList == null || keyList.isEmpty()) {
                    continue;
                }
                jedis = pool.getResource();
                Pipeline p = jedis.pipelined();

                Map<String, Response<String>> responseMap = new LinkedHashMap<>(keyList.size());
                for (String key: keyList) {
                    Response<String> response = p.set(key, keyValues.get(key));
                    responseMap.put(key, response);
                }
                p.sync();
                for (String key: responseMap.keySet()) {
                    resultResponse.put(key, responseMap.get(key).get());
                }
            } catch (Exception e) {
                e.printStackTrace();        // use log
                isBroken = true;
            } finally {
                if (isBroken) {
                    pool.returnBrokenResource(jedis);
                } else {
                    pool.returnResource(jedis);
                }
                pool.destroy();
            }
        }
        return resultResponse;
    }

    /**
     * mget:
     * @param keys
     * @return  [key:response] map
     */
    public Map<String, String> mget(final List<String> keys) {
        Map<JedisPool, List<String>> poolKeysMap = getPoolKeysMap(keys);
        Map<String, String> resultResponse = new LinkedHashMap<>();
        if (poolKeysMap == null) {
            return null;
        }

        for (JedisPool pool: poolKeysMap.keySet()) {
            Jedis jedis = null;
            boolean isBroken = false;
            try {
                List<String> keyList = poolKeysMap.get(pool);
                if (keyList == null || keyList.isEmpty()) {
                    continue;
                }
                jedis = pool.getResource();
                Pipeline p = jedis.pipelined();
                Map<String, Response<String>> responseMap = new LinkedHashMap<>(keyList.size());
                for (String key: keyList) {
                    Response<String> response = p.get(key);
                    responseMap.put(key, response);
                }
                p.sync();
                for (Map.Entry<String, Response<String>> entry: responseMap.entrySet()) {
                    resultResponse.put(entry.getKey(), entry.getValue().get());
                }
            } catch (Exception e) {
                e.printStackTrace();
                isBroken = true;
            } finally {
                if (isBroken) {
                    pool.returnBrokenResource(jedis);
                } else {
                    pool.returnResource(jedis);
                }
                pool.destroy();
            }
        }
        return resultResponse;
    }

    /**
     * put keys into nodes using CRC16, and return [jedisPool:keys] map
     *
     * @param keys
     * @return
     */
    public Map<JedisPool, List<String>> getPoolKeysMap(final List<String> keys) {
        Map<JedisPool, List<String>> poolListMap = new LinkedHashMap<>();
        for (String key: keys) {
            int slotOfKey = JedisClusterCRC16.getSlot(key);
            JedisPool jedisPool = connectionHandler.getSlotPool(slotOfKey);
            List<String> keyList = poolListMap.get(jedisPool);
            if (keyList == null) {
                keyList = new ArrayList<>();
                keyList.add(key);
                poolListMap.put(jedisPool, keyList);
            } else {
                keyList.add(key);
            }
        }
        return poolListMap;
    }

    /**
     * extend the handler to get pool from slot
     */
    static class CustomConnectionHandler extends JedisSlotBasedConnectionHandler {
        public CustomConnectionHandler(Set<HostAndPort> nodes, final GenericObjectPoolConfig poolConfig) {
            super(nodes, poolConfig);
        }

        public JedisPool getSlotPool(final int slot) {
            return cache.getSlotPool(slot);
        }
    }
}
