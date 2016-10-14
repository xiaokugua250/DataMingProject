package com.morty.java.dmp.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Created by duliang on 2016/6/9.
 */
public class WordCountTopolopyMain {
    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word-reader", new WordReadSpout());
        builder.setBolt("word-normalizer", new WordNormalizerBolt()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCountStorm(), 2).fieldsGrouping("word-normailizer", new Fields("word"));

        // ------------configuration--------------
        Config config = new Config();

        config.put("wordsFile", args[0]);
        config.setDebug(false);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("getting-start-topolgie", config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
