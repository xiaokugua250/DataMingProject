package com.morty.java.dmp.hadoopElasticsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by morty on 2016/10/14.
 */
public class SparkElasticConfig {

    private static final String ES_SERVER = "es-server:9200";
    private static final String ES_RESOURCE = "radio/artists";
    private static SparkConf sparkConf;
    private static JavaSparkContext javaSparkContext;

    public void init() {
        sparkConf = new SparkConf();
        javaSparkContext = new JavaSparkContext(sparkConf);
    }
}
