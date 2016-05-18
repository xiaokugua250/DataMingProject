package com.morty.java.dmp.spark;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.ResourceBundle;

/**
 * Created by duliang on 2016/5/15.
 */
public class sparkWordCountDemo {

    Logger LOG=Logger.getLogger(sparkWordCountDemo.class);
    private static SparkConf conf;
    private static  ResourceBundle bundle;
    private static JavaSparkContext javaSparkContext;
     static{
        try{
            bundle= ResourceBundle.getBundle("spark-configuration");
            if(bundle == null){
                throw new IllegalArgumentException("spark-configuration.propertis is not found");
            }
            conf=new SparkConf();
            conf.setMaster(bundle.getString("spark.master"));
            conf.setAppName(bundle.getString("spark.app.name"));
            javaSparkContext=new JavaSparkContext(conf);

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     *
     * @param fileName
     * @param output
     */
    public static void  wordCountJava8(String fileName,String output){

        JavaRDD<String> input=javaSparkContext.textFile(fileName);

        JavaRDD<String> words=input.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        //Map
        JavaPairRDD<String,Integer> pairs=words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return  new Tuple2<String, Integer>(s,1);
            }
        });

        //reduce
        JavaPairRDD<String,Integer> counts=pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return  integer+integer2;
            }
        });

        //循环输出 或其他自定义操作
       /* counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("stringIntegerTuple2._1()+\":\"+stringIntegerTuple2._2() = " + stringIntegerTuple2._1()+":"+stringIntegerTuple2._2());
            }
        });*/


        counts.saveAsTextFile(output);
        //save as textfile


    }

    /**
     *
     * @param pairRDD
     * @param output
     */
    public static void saveText(JavaPairRDD<String,Integer> pairRDD,String output){

        try {
            //save as textfile
            pairRDD.saveAsTextFile(output);

           // pairRDD.saveAsHadoopFile();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     *
     * @param javaSparkContext
     */
    public static void closeSpark(JavaSparkContext javaSparkContext){
        try {
            javaSparkContext.close();
        } finally {
            javaSparkContext.close();
        }
    }


}
