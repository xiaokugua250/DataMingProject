package com.morty.java.dmp.elasticsearch.aggs;

public class StatsAgg extends YiliAggsEntity {

    public StatsAgg(String aggName) {
        setAggName(aggName);
        setAggType(YiliAggsEntity.STATS_TYPE);
    }
}
