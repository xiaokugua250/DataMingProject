package com.morty.java.dmp.storm;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by duliang on 2016/6/9.
 */
public class WordReadSpout implements IRichSpout {
    private boolean completed = false;
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private TopologyContext context;

    @Override
    public void ack(Object o) {
        System.out.println("oK = " + o);
    }

    @Override
    public void activate() {
    }

    @Override
    public void close() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public void fail(Object o) {
        System.out.println("FAILE = " + o);
    }

    @Override
    public void nextTuple() {

        /**
         *
         * The nextuple it is called forever, so if we have beenreaded the file
         *
         * we will wait and then return
         *
         */
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            return;
        }

        String str;
        BufferedReader reader = new BufferedReader(fileReader);

        try {
            while ((str = reader.readLine()) != null) {
                this.collector.emit(new Values(str), str);
            }
        } catch (IOException e) {
            e.printStackTrace();

            throw new RuntimeException("error tupe", e);
        } finally {
            completed = true;
        }
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.context = topologyContext;
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        } catch (Exception e) {
            e.printStackTrace();
        }

        this.collector = spoutOutputCollector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
