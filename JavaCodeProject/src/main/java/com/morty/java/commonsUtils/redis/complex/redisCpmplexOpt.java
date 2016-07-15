package com.morty.java.commonsUtils.redis.complex;

import redis.clients.jedis.*;

import java.util.List;

/**
 * Created by duliang on 2016/6/14.
 */
public class redisCpmplexOpt {

    public void init() {
        // TODO: 2016/6/14  初始化init 
    }

    //---------------------事务--------------------------------------
    public Transaction getTransaction(Jedis jedis, String... params) {
        // TODO: 2016/6/14  获取事务 
        Transaction transaction = jedis.multi();
        return transaction;
    }

    public void getTransactionExec(Transaction transaction, String... params) {
        // TODO: 2016/6/14  执行事务 
        transaction.exec();
    }


    //----------------管道--------------------------

    public Pipeline getPipeLine(Jedis jedis, String... params) {
        // TODO: 2016/6/14  获取管道
        Pipeline pipeline = jedis.pipelined();
        return pipeline;
    }

    public void getPipeLineExec(Pipeline pipeline, String... params) {
        // TODO: 2016/6/14  管道操作
        pipeline.syncAndReturnAll();
        // pipeline.sync();

    }

    //----------------------管道中调用事务----------------
    public void pipeLineTrans(Pipeline pipeline, String... params) {
        // TODO: 2016/6/14  get pipeline mutix
        pipeline.multi();
    }

    public void pipeLineTransExec(Pipeline pipeline, String params) {
        // TODO: 2016/6/14  执行
        pipeline.exec();
    }

    //---------------分布式----------
    public ShardedJedis getSharedJedis(List<JedisShardInfo> shareds, String... params) {
        // TODO: 2016/6/14  获取分布式shareds
        ShardedJedis sharedJedis = new ShardedJedis(shareds);
        // sharededJedisPool.getResource();
        return sharedJedis;
    }

    public ShardedJedisPool getSharedJedisP(List<JedisShardInfo> shareds, String... params) {
        // TODO: 2016/6/14  获取分布式sharedspool
        ShardedJedisPool sharedJedispool = new ShardedJedisPool(new JedisPoolConfig(), shareds);
        return sharedJedispool;
    }

    public void SharedJedisOpt(ShardedJedis shardedJedis, String... params) {
        // TODO: 2016/6/14  分布式同步调用
        //  shardedJedis.xxx();
    }

    public ShardedJedisPipeline getSharedJedisPipeline(ShardedJedis sharedJedis, String... params) {
        ShardedJedisPipeline shardedJedisPipeLine = sharedJedis.pipelined();
        return shardedJedisPipeLine;
    }

    public void shardedJedisPipeLineExec(ShardedJedisPipeline shardedJedisPipeLine, String... params) {
        // TODO: 2016/6/14  分布式异步调用
        shardedJedisPipeLine.syncAndReturnAll();

    }


}
