package com.morty.java.commonsUtils.redis.complex;

import redis.clients.jedis.*;

import java.util.List;

/**
 * Created by duliang on 2016/6/14.
 */
public class redisCpmplexOpt {
    public void SharedJedisOpt(ShardedJedis shardedJedis, String... params) {

        // TODO: 2016/6/14  �ֲ�ʽͬ������
        // shardedJedis.xxx();
    }

    public void init() {

        // TODO: 2016/6/14  ��ʼ��init
    }

    // ----------------------�ܵ��е�������----------------
    public void pipeLineTrans(Pipeline pipeline, String... params) {

        // TODO: 2016/6/14  get pipeline mutix
        pipeline.multi();
    }

    public void pipeLineTransExec(Pipeline pipeline, String params) {

        // TODO: 2016/6/14  ִ��
        pipeline.exec();
    }

    public void shardedJedisPipeLineExec(ShardedJedisPipeline shardedJedisPipeLine, String... params) {

        // TODO: 2016/6/14  �ֲ�ʽ�첽����
        shardedJedisPipeLine.syncAndReturnAll();
    }

    // ----------------�ܵ�--------------------------
    public Pipeline getPipeLine(Jedis jedis, String... params) {

        // TODO: 2016/6/14  ��ȡ�ܵ�
        Pipeline pipeline = jedis.pipelined();

        return pipeline;
    }

    public void getPipeLineExec(Pipeline pipeline, String... params) {

        // TODO: 2016/6/14  �ܵ�����
        pipeline.syncAndReturnAll();

        // pipeline.sync();
    }

    // ---------------�ֲ�ʽ----------
    public ShardedJedis getSharedJedis(List<JedisShardInfo> shareds, String... params) {

        // TODO: 2016/6/14  ��ȡ�ֲ�ʽshareds
        ShardedJedis sharedJedis = new ShardedJedis(shareds);

        // sharededJedisPool.getResource();
        return sharedJedis;
    }

    public ShardedJedisPool getSharedJedisP(List<JedisShardInfo> shareds, String... params) {

        // TODO: 2016/6/14  ��ȡ�ֲ�ʽsharedspool
        ShardedJedisPool sharedJedispool = new ShardedJedisPool(new JedisPoolConfig(), shareds);

        return sharedJedispool;
    }

    public ShardedJedisPipeline getSharedJedisPipeline(ShardedJedis sharedJedis, String... params) {
        ShardedJedisPipeline shardedJedisPipeLine = sharedJedis.pipelined();

        return shardedJedisPipeLine;
    }

    // ---------------------����--------------------------------------
    public Transaction getTransaction(Jedis jedis, String... params) {

        // TODO: 2016/6/14  ��ȡ����
        Transaction transaction = jedis.multi();

        return transaction;
    }

    public void getTransactionExec(Transaction transaction, String... params) {

        // TODO: 2016/6/14  ִ������
        transaction.exec();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
