package com.morty.java.commonsUtils.redis.sub;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * Created by duliang on 2016/6/14.
 */
public class redisPuSubThread implements Runnable {
    private final redisPubSub redisPubSub = new redisPubSub();
    private final String channel = "mychannel";
    private final JedisPool jedisPool;

    public redisPuSubThread(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public void run() {
        System.out.println(String.format("subscribe redis, channel %s, thread will be blocked", channel));

        Jedis jedis = null;

        try {
            jedis = jedisPool.getResource();
            jedis.subscribe(redisPubSub, channel);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(String.format("subsrcibe channel error, %s", e));
        } finally {
            if (jedis != null) {
                jedis.close();
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
