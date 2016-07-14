package com.morty.java.dmp.spark;

/**
 * Created by morty on 2016/06/21.
 */
public class SimpleSparkApp {
    /*public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf();
        conf.setAppName("simpleApp");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String[]> data = javaSparkContext.textFile("data/UserPurchaseHistory.csv")
                .map(new Function<String, String[]>() {
                    @Override
                    public String[] call(String v1) throws Exception {
                        return v1.split(",");
                    }
                });

        Long numPurchases = data.count();
        long uniqueUsers = data.map(new Function<String[], java.lang.String>() {
            @Override
            public java.lang.String call(String[] v1) throws Exception {
                return v1[0];
            }
        }).distinct().count();

        double totalRevenuse = data.map(new DoubleFunction<String[]>() {

            @Override
            public double call(String[] strings) throws Exception {
                return Double.parseDouble(strings[2]);
            }
        });
        List<Tuple2<String[], Integer>> pair = data.map(new PairFunction<String[], String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String[] strings) throws Exception {
                return new Tuple2<String, Integer>(String[1], 1);
            }
        }).reduceByKey(new Function2<>() {
            @Override
            public Object call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).collect();

    }
*/
}
