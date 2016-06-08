package com.morty.java.dmp.elasticsearch.aggs;

public class DateAgg extends EsAggsEntity {

    private String intervalStr = "day";
    private String offsetStr = "+0d";

    public DateAgg(String aggName, String intervalStr, String offsetStr, int aggSize) {
        setAggName(aggName);
        setIntervalStr(intervalStr);
        setOffsetStr(offsetStr);
        setAggSize(aggSize);
        setAggType(EsAggsEntity.DATE_TYPE);
    }
    
    public String getIntervalStr() {
        return intervalStr;
    }
    public void setIntervalStr(String intervalStr) {
        this.intervalStr = intervalStr;
    }

    public String getOffsetStr() {
        return offsetStr;
    }
    public void setOffsetStr(String offsetStr) {
        this.offsetStr = offsetStr;
    }
}
