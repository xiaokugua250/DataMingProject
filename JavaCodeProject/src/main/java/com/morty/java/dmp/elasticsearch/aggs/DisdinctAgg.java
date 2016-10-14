package com.morty.java.dmp.elasticsearch.aggs;

public class DisdinctAgg extends EsAggsEntity {
    private Long precisionThresholdStr = 40000L;

    public DisdinctAgg(String aggName, Long precisionThresholdStr, int aggSize) {
        setAggName(aggName);
        setPrecisionThresholdStr(precisionThresholdStr);
        setAggSize(aggSize);
        setAggType(EsAggsEntity.DISDINCT_TYPE);
    }

    public Long getPrecisionThresholdStr() {
        return precisionThresholdStr;
    }

    public void setPrecisionThresholdStr(Long precisionThresholdStr) {
        this.precisionThresholdStr = precisionThresholdStr;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
