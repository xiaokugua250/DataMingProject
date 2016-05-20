package com.morty.java.dmp.elasticsearch.aggs;

/**
 * Created by Administrator on 2016/4/15.
 */
public class SumAgg extends YiliAggsEntity {
    
    public SumAgg(String aggName) {
        setAggName(aggName);
        setAggType(YiliAggsEntity.SUM_TYPE);
    }
}
