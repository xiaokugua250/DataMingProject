package com.morty.java.dmp.elasticsearch.aggs;

import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

public class NormalAgg extends EsAggsEntity {
    private AggregationBuilder aggregationBuilder;

    public NormalAgg(String aggName, int aggSize) {
        setAggName(aggName);
        setAggSize(aggSize);
        setAggType(EsAggsEntity.NORMAL_TYPE);
    }

    /**
     *  aggsʱ  terms include ��exclude
     * @param aggName
     * @param termsStr
     * @param include
     */
    public NormalAgg(String aggName, String termsStr, boolean include) {
        setAggName(aggName);

        if (include == true) {
            setIncludeStr(termsStr);
            setAggType(EsAggsEntity.INCLUDE_TYPE);
        } else {
            setExcludeStr(termsStr);
            setAggType(EsAggsEntity.EXCLUDE_TYPE);
        }
    }

    public void initAggregationBuilder() {
        setAggregationBuilder(AggregationBuilders.terms(this.getAggName())
                .field(this.getAggName())
                .size(this.getAggSize()));
    }

    public AggregationBuilder getAggregationBuilder() {
        return aggregationBuilder;
    }

    public void setAggregationBuilder(AggregationBuilder aggregationBuilder) {
        this.aggregationBuilder = aggregationBuilder;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
