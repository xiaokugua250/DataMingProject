package com.morty.java.dmp.elasticsearch.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

public class NormalAgg extends YiliAggsEntity {

    private AggregationBuilder aggregationBuilder;

    public NormalAgg(String aggName, int aggSize) {
        setAggName(aggName);
        setAggSize(aggSize);
        setAggType(YiliAggsEntity.NORMAL_TYPE);
    }

    /**
     *  aggs时  terms include 和exclude
     * @param aggName
     * @param termsStr
     * @param include
     */
    public NormalAgg(String aggName,String termsStr,boolean include){
        setAggName(aggName);

        if(include == true) {
            setIncludeStr(termsStr);
            setAggType(YiliAggsEntity.INCLUDE_TYPE);
        }else {
            setExcludeStr(termsStr);
            setAggType(YiliAggsEntity.EXCLUDE_TYPE);
        }
    }


    public AggregationBuilder getAggregationBuilder() {
        return aggregationBuilder;
    }
    public void setAggregationBuilder(AggregationBuilder aggregationBuilder) {
        this.aggregationBuilder = aggregationBuilder;
    }

    public void initAggregationBuilder() {
        setAggregationBuilder(AggregationBuilders.terms(this.getAggName()).field(this.getAggName()).size(this.getAggSize()));
    }
}
