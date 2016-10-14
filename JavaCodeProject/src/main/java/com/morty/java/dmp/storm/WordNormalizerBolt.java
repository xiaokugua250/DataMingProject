package com.morty.java.dmp.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by duliang on 2016/6/9.
 */
public class WordNormalizerBolt implements IRichBolt {
    private OutputCollector collector;

    @Override
    public void cleanup() {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        String[] words = sentence.split(" ");

        for (String word : words) {
            word = word.trim();

            if (!word.isEmpty()) {
                word = word.toLowerCase();

                List a = new ArrayList();

                a.add(tuple);
                collector.emit(a, new Values(word));
            }
        }

        collector.ack(tuple);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
