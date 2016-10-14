package com.morty.java.commonsUtils.redis.test;

import com.morty.java.commonsUtils.redis.RedisInitOpt;
import com.morty.java.commonsUtils.redis.sub.redisPuSubThread;
import com.morty.java.commonsUtils.redis.sub.redisPublish;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by duliang on 2016/6/7.
 */
public class RedisTest {
    @Test
    public void RedisInitOptTest() {
        RedisInitOpt initOpt = new RedisInitOpt();
        Jedis jedis = RedisInitOpt.getJedis(true);
        String ping = initOpt.isPing(jedis);

        initOpt.keyValueRedis("jedis", "jediskeyvalue", jedis);

        Set<String> settest = new HashSet();

        settest.add("hello");
        settest.add("word");
        settest.add("duliang");
        settest.add("hello");
        initOpt.setRedis(settest, jedis);
        System.out.println("ping = " + ping);
    }

    @Test
    public void RedisPubSubTest() {
        String redisIp = "192.168.30.50";
        int reidsPort = 6379;
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, reidsPort);
        redisPuSubThread redisPuSubThread = new redisPuSubThread(jedisPool);
        Thread thread = new Thread(redisPuSubThread);

        thread.start();

        redisPublish publish = new redisPublish(jedisPool);

        publish.start();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
