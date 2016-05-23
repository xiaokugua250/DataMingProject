package com.morty.java.dmp.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Map;

/**
 *  pair-key average using combineByKey()
 * Created by morty on 2016/05/23.
 */
public class SparkAvgByCombineByKey {


    //map
    Function<Integer,SparkAvgCount> createAcc=new Function<Integer, SparkAvgCount>() {
        public SparkAvgCount call(Integer v1) throws Exception {
            return new SparkAvgCount(v1,1);
        }
    };


    //merge
    Function2<SparkAvgCount,Integer,SparkAvgCount> addAndCount=new Function2<SparkAvgCount, Integer, SparkAvgCount>() {
        public SparkAvgCount call(SparkAvgCount v1, Integer v2) throws Exception {
            v1.total_+=v2;
            v1.num_+=1;
            return v1;
        }
    };


    //combine
    Function2<SparkAvgCount,SparkAvgCount,SparkAvgCount> combine=new Function2<SparkAvgCount, SparkAvgCount, SparkAvgCount>() {
        public SparkAvgCount call(SparkAvgCount v1, SparkAvgCount v2) throws Exception {
            v1.total_+=v2.total_;
            v1.num_+=v1.num_;
            return v1;
        }
    };


  /*
    public <C> JavaPairRDD<K,C> combineByKey(Function<V,C> createCombiner,
                                Function2<C,V,C> mergeValue,
                                Function2<C,C,C> mergeCombiners)
Simplified version of combineByKey that hash-partitions the resulting RDD using the existing partitioner/parallelism level and using map-side aggregation.

    SparkAvgCount initial=new SparkAvgCount(0,0);
    JavaPairRDD<String,SparkAvgCount> avgCounts=nums.combineByKey(createAcc,addAndCount,combine);
    Map<String,SparkAvgCount> countMap=avgCounts.collectAsMap();
    for(Map.Entry<String,SparkAvgCount> entry : countMap.entrySet()){
        System.out.println("entry = " + entry);
    }*/
}
