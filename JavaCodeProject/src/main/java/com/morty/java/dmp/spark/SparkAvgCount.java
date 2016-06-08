package com.morty.java.dmp.spark;

import org.apache.log4j.Logger;

import java.io.Serializable;

/**
 * Created by morty on 2016/05/23.
 */
public class SparkAvgCount implements Serializable {

    public int total_;
    public int num_;
    Logger LOG = Logger.getLogger(SparkAvgCount.class.getName());

    public SparkAvgCount(int total, int num) {
        total_ = total;
        num_ = num;
    }

    public float avg() {
        return total_ / (float) num_;
    }


}

