package com.morty.java.commonsUtils.redis.test;

import com.morty.java.commonsUtils.redis.RedisInitOpt;
import org.junit.Test;
import redis.clients.jedis.Jedis;

/**
 * Created by duliang on 2016/6/7.
 */
public class RedisTest {

    @Test
    public void RedisInitOptTest() {

        RedisInitOpt initOpt = new RedisInitOpt();
        initOpt.init();

        Jedis jedis = initOpt.getJedis(true);

        String ping = initOpt.isPing(jedis);
        System.out.println("ping = " + ping);
    }
}
