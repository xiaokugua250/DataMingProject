package com.morty.java.dmp.elasticsearch.aggs;

/**
 * ES�ۺϲ�ѯ����
 * morty 2016/05/20
 */
public class EsAggsEntity {
    public static final int NORMAL_TYPE = 1;
    public static final int DATE_TYPE = 2;
    public static final int DISDINCT_TYPE = 3;
    public static final int STATS_TYPE = 4;
    public static final int NESTED_TYPE = 5;
    public static final int SUM_TYPE = 6;

    // ------ aggration include /exclude---------
    public static final int INCLUDE_TYPE = 7;
    public static final int EXCLUDE_TYPE = 8;
    private String aggName;
    private int aggType;
    private int aggSize;
    private String includeStr;
    private String excludeStr;

    public String getAggName() {
        return aggName;
    }

    public void setAggName(String aggName) {
        this.aggName = aggName;
    }

    public int getAggSize() {
        return aggSize;
    }

    public void setAggSize(int aggSize) {
        this.aggSize = aggSize;
    }

    public int getAggType() {
        return aggType;
    }

    public void setAggType(int aggType) {
        this.aggType = aggType;
    }

    public String getExcludeStr() {
        return excludeStr;
    }

    public void setExcludeStr(String excludeStr) {
        this.excludeStr = excludeStr;
    }

    public String getIncludeStr() {
        return includeStr;
    }

    public void setIncludeStr(String includeStr) {
        this.includeStr = includeStr;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
