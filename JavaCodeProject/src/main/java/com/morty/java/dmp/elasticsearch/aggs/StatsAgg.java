package com.morty.java.dmp.elasticsearch.aggs;

public class StatsAgg extends EsAggsEntity {
    public StatsAgg(String aggName) {
        setAggName(aggName);
        setAggType(EsAggsEntity.STATS_TYPE);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
