package com.morty.java.dmp.hadoopElasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.morty.java.beans.User;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Spark Es操作
 * Created by morty on 2016/10/14.
 */
public class SparkElastic {


    public void SparkEsWrite(JavaRDD javaRDD) {
        Map<String, ?> numbers = ImmutableMap.of("one", 1, "two", 2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
        JavaEsSpark.SaveToEs(javaRDD, "sparkIndex/docs");
    }

    /**
     * 写Java Bean 到Es
     *
     * @param javaRDD
     * @param user
     */
    public void SparkEsWrite(JavaSparkContext javaSparkContext, JavaRDD javaRDD, User user) {
        List<User> userList = new LinkedList<User>();
        userList.add(user);
        JavaRDD<User> javaRDDUser = javaSparkContext.parallelize(
                ImmutableList.<User>of((User) userList));

        javaRDDUser.saveToEs(javaRDDUser, "sparkIndex/docs");
    }
}
