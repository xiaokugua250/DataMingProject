package com.morty.java.commonsUtils.redis.sub;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by duliang on 2016/6/14.
 */
public class redisPublish {
    private final JedisPool jedisPool;


    public redisPublish(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    public void start() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        Jedis jedis = jedisPool.getResource();
        while (true) {
            String line = null;
            try {
                line = reader.readLine();
                if (!"quit".equals(line)) {
                    jedis.publish("mychannel", line);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
