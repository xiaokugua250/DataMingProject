package com.morty.java.commonsUtils.redis.sub;

import redis.clients.jedis.Client;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 * redis ���ķ���
 * Created by duliang on 2016/6/14.
 */
public class redisPubSub extends JedisPubSub {
    public static void main(String[] args) {
        String redisIp = "127.0.0.1";
        int reidsPort = 6379;
        JedisPool jedisPool = new JedisPool(new JedisPoolConfig(), redisIp, reidsPort);
        redisPuSubThread redisPuSubThread = new redisPuSubThread(jedisPool);
        Thread thread = new Thread(redisPuSubThread);

        thread.start();

        redisPublish publish = new redisPublish(jedisPool);

        publish.start();
    }

    @Override
    public void onMessage(String channel, String message) {
        System.out.println("channel = " + channel + "message:" + message);

        if (message.equals("quit")) {
            this.unsubscribe(channel);
        }
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        super.onPSubscribe(pattern, subscribedChannels);
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        super.onPUnsubscribe(pattern, subscribedChannels);
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        super.onSubscribe(channel, subscribedChannels);
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        super.onUnsubscribe(channel, subscribedChannels);
    }

    @Override
    public void proceed(Client client, String... channels) {
        super.proceed(client, channels);
    }

    @Override
    public void proceedWithPatterns(Client client, String... patterns) {
        super.proceedWithPatterns(client, patterns);
    }

    @Override
    public void psubscribe(String... patterns) {
        super.psubscribe(patterns);
    }

    @Override
    public void punsubscribe() {
        super.punsubscribe();
    }

    @Override
    public void punsubscribe(String... patterns) {
        super.punsubscribe(patterns);
    }

    @Override
    public void subscribe(String... channels) {

        // TODO: 2016/6/14  ����channel
        super.subscribe(channels);
    }

    @Override
    public void unsubscribe() {
        super.unsubscribe();
    }

    @Override
    public void unsubscribe(String... channels) {

        // TODO: 2016/6/14  ȡ������
        super.unsubscribe(channels);
    }

    @Override
    public boolean isSubscribed() {
        return super.isSubscribed();
    }

    @Override
    public int getSubscribedChannels() {
        return super.getSubscribedChannels();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
