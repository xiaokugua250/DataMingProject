package com.morty.java.dmp.spark;

import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by morty on 2016/05/23.
 */
public class SparkAvgCount implements Serializable {

    Logger LOG=Logger.getLogger(SparkAvgCount.class.getName());

    public int total_;
    public int num_;
    public SparkAvgCount(int total,int num){
        total_=total;
        num_=num;
    }
    public float avg(){
        return total_/(float)num_;
    }


}

