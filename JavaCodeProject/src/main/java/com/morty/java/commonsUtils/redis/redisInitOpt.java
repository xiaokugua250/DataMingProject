package com.morty.java.commonsUtils.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ResourceBundle;

/**
 * Created by duliang on 2016/5/14.
 */
public class redisInitOpt {

    public  Jedis jedis;
    public JedisPoolConfig config;
    public ResourceBundle bundle;
    public void init(){


        bundle=ResourceBundle.getBundle("redis-commons");
        if(bundle == null){
            throw new IllegalArgumentException("redis.propertis is not found");
        }
        //TODO redis 配置信息
        config=new JedisPoolConfig();
        config.setMaxIdle(Integer.valueOf(bundle.getString("reids.pool.maxIdle")));
        config.setMaxTotal(Integer.valueOf(bundle.getString("redis.pool.maxActive")));
        config.setMaxWaitMillis(Integer.valueOf(bundle.getString("redis.pool.maxWait")));

    }



    /**
     * 获取Redis 是否采用池的方式获取
     * @param host
     * @param isPool
     * @return
     */
    public Jedis getJedis(String host,int port,boolean isPool){
        if (isPool == false) {

            jedis=new Jedis(host,port);

            return  jedis;

        } else {

            JedisPool pool=new JedisPool(new JedisPoolConfig(),host,port);
            jedis=pool.getResource();

            return jedis;
        }
    }
}
