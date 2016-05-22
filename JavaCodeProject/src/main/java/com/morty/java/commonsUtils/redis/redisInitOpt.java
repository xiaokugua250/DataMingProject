package com.morty.java.commonsUtils.redis;

import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.SortingParams;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

/**
 * Created by duliang on 2016/5/14.
 */
public class RedisInitOpt {
    static Logger LOG=Logger.getLogger(RedisInitOpt.class);

    private static  Jedis jedis;
    private static JedisPool pool;
    public JedisPoolConfig config;
    public ResourceBundle bundle;

    /**
     * 获取Redis 是否采用池的方式获取
     * @param isPool
     * @return
     */
    public  synchronized  static  Jedis getJedis(boolean isPool){

        try {
            if(pool !=null){
                jedis=pool.getResource();
                return jedis;
            }else {
                return  null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("get jedis error:"+e.getMessage());
            return null;

        }

    }

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
        pool=new JedisPool(new JedisPoolConfig(), RedisInfo.HOST, RedisInfo.PORT);

    }

    /**
     *
     * @param key
     * @param value
     * @param jedis
     */
    public void keyValueRedis(String  key,String value,Jedis jedis){
        try {
            jedis.set(System.currentTimeMillis()+key,value);
            //jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("store keyvalue error:"+e.getMessage());
        }
    }


    /**
     * 存储对象时需要进行序列化
     * @param obj
     * @param jedis
     */
    public void ObjectRedis(Beans<String> obj, Jedis jedis){

        jedis.flushDB();
        byte[] objBytes=SerializationUtils.serialize(obj);  //序列化
        jedis.set(objBytes,objBytes);
       // jedis.get(objBytes);
    }

    /**
     *
     * @param list
     * @param jedis
     */
    public void listRedis(List<?> list, Jedis jedis ){

        jedis.flushDB();
        jedis.lpush(System.currentTimeMillis()+list.toString(),list.toArray(new  String[list.size()]));
        /*jedis.lpush("collections", "HashSet");
        jedis.lpush("collections", "TreeSet");
        jedis.lpush("collections", "TreeMap");*/

    }

    /**
     *
     * @param set
     * @param jedis
     */
    public void setRedis(Set<?> set,Jedis jedis){

        jedis.flushDB();
        jedis.sadd(System.currentTimeMillis()+set.toString(),set.toArray(new String[set.size()]));
    }

    /**
     *
     * @param map
     * @param jedis
     */
    public void hashRedis(Map<?,?>map,Jedis jedis){

        jedis.flushDB();
        jedis.hmset(System.currentTimeMillis()+map.toString(), (Map<String, String>) map);

    }

    /**
     *
     * @param list
     * @param jedis
     */
    public void sortRedis(List<?> list,Jedis jedis){
        jedis.flushDB();
        jedis.lpush(System.currentTimeMillis()+list.toString(),list.toArray(new String [list.size()]));

        SortingParams sortingParams=new SortingParams();
        jedis.sort(System.currentTimeMillis()+list.toString(),sortingParams.alpha());

    }

    /**
     *
     * @param jedis
     * @return
     */
    public String  isPing(Jedis jedis){

        try {
            return  jedis.ping();
        } catch (Exception e) {
            e.printStackTrace();
            return "ERROR" ;
        }
    }


    /**
     *
     * @param jedis
     */
    public void flushReids(Jedis jedis){
        try {
            jedis.flushDB();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
