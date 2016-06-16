package com.morty.java.dmp.elasticsearch.aggs;

/**
 * Created by xiangmin on 2016/4/7.
 *
 * 这里的Nested聚合与ES官网所说的Nested Aggregate相符，NewYZSearch里的Nested Aggregate是多层聚合！
 */
public class NestedAgg extends EsAggsEntity {

    public NestedAgg(String aggName, int aggSize) {
        setAggName(aggName);
        setAggSize(aggSize);
        setAggType(EsAggsEntity.NESTED_TYPE);
    }
}
