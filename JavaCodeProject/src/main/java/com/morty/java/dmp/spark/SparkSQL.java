package com.morty.java.dmp.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * spark sql 操作
 * Created by morty on 2016/05/25.
 */
public class SparkSQL {

    public JavaSparkContext javaSparkContext;
    public SparkConf sparkConf;
    public SQLContext sqlContext;
    Logger LOG = Logger.getLogger(SparkSQL.class.getName());

    public void init() {
        sparkConf = new SparkConf();

        javaSparkContext = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(javaSparkContext);


    }


}
