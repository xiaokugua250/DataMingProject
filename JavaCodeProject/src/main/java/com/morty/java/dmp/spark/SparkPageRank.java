package com.morty.java.dmp.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import com.google.common.collect.Iterables;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;


import java.util.ResourceBundle;

/**
 * * Computes the PageRank of URLs from an input file. Input file should
 * be in format of:
 * URL         neighbor URL
 * URL         neighbor URL
 * URL         neighbor URL
 * ...
 * where URL and their neighbors are separated by space(s).
 * Created by morty on 2016/05/23.
 */
public class SparkPageRank {

    private static final Pattern SPACES=Pattern.compile("\\s+");

    private static SparkConf conf;
    private static ResourceBundle bundle;
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
    private static class Sum implements Function2<Double,Double,Double>{
        public Double call(Double v1, Double v2) throws Exception {
            return v1+v2;
        }
    }

    public static  void pageRanke(String pageFile,String iterNum) {
        // Loads in input file. It should be in format of:
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     URL         neighbor URL
        //     ...
        JavaRDD<String> lines = javaSparkContext.textFile(pageFile);


        // Loads all URLs from input file and initialize their neighbors.
        JavaPairRDD<String, Iterable<String>> links = lines.mapToPair(
                new PairFunction<String,String,String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] parts=SPACES.split(s);
                        return new Tuple2<String, String>(parts[0],parts[1]);
                    }
                }
        ).distinct().groupByKey().cache();

        // Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
        JavaPairRDD<String,Double> ranks=links.mapValues(new Function<Iterable<String>, Double>() {
            public Double call(Iterable<String> v1) throws Exception {
                return 1.0;
            }
        });

        // Calculates and updates URL ranks continuously using PageRank algorithm.
        for(int current=0;current<Integer.parseInt(iterNum);current++){
            // Calculates URL contributions to the rank of other URLs.
            JavaPairRDD<String, Double> contribs = links.join(ranks).values()
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                        @Override
                        public Iterable<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> iterableDoubleTuple2) throws Exception {
                            int urlCount = Iterables.size(iterableDoubleTuple2._1);
                            List<Tuple2<String, Double>> results = new ArrayList<>();
                            for (String n : iterableDoubleTuple2._1) {
                                results.add(new Tuple2<>(n, iterableDoubleTuple2._2() / urlCount));
                            }
                            return (Iterable<Tuple2<String, Double>>) results.iterator();
                        }
                    });

            // Re-calculates URL ranks based on neighbor contributions.
            ranks=contribs.reduceByKey(new Sum()).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double v1) throws Exception {
                    return 0.15+v1*0.85;
                }
            });
        }
        List<Tuple2<String,Double>> output=ranks.collect();
        for(Tuple2<?,?> tuple2 :output){
            System.out.println( tuple2._1()+" has rank "+tuple2._2()+" . ");
        }
    }





    public static void main(String[] args) throws  Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaPageRank <file> <number_of_iterations>");
            System.exit(1);
        }
        pageRanke(args[0],args[1]);
    }

}

