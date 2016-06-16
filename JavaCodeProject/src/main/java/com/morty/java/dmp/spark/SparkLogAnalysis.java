package com.morty.java.dmp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by morty on 2016/05/23.
 * https://www.ibm.com/developerworks/cn/opensource/os-cn-spark-code-samples/
 */
public class SparkLogAnalysis {

    public static final Pattern apacheLogRegx = Pattern.compile(
            "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] (.+?)\" (\\d{3}) ([\\d\\-]+) \"([^\"]+)\" \"([^\"]+)\".*");

    public static Tuple3<String, String, String> extractKey(String line) {
        Matcher m = apacheLogRegx.matcher(line);
        if (m.find()) {
            String ip = m.group(1);
            String user = m.group(3);
            String query = m.group(5);
            if (!user.equalsIgnoreCase("-")) {
                return new Tuple3<String, String, String>(ip, user, query);
            }

        }
        return new Tuple3<>(null, null, null);
    }

    public static Stats extractStats(String line) {
        Matcher m = apacheLogRegx.matcher(line);
        if (m.find()) {
            int bytes = Integer.parseInt(m.group(7));
            return new Stats(1, bytes);
        } else {
            return new Stats(1, 0);
        }
    }

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ApacheLogAnalysis");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> dataSet = (args.length == 1) ? javaSparkContext.textFile(args[0]) : javaSparkContext.parallelize(SparkInfo.ApacheLogs);

        JavaPairRDD<Tuple3<String, String, String>, Stats> extracted = dataSet.mapToPair(new PairFunction<String, Tuple3<String, String, String>, Stats>() {
            @Override
            public Tuple2<Tuple3<String, String, String>, Stats> call(String s) throws Exception {
                return new Tuple2<Tuple3<String, String, String>, Stats>(extractKey(s), extractStats(s));
            }
        });

        JavaPairRDD<Tuple3<String, String, String>, Stats> counts = extracted.reduceByKey(new Function2<Stats, Stats, Stats>() {
            @Override
            public Stats call(Stats v1, Stats v2) throws Exception {
                return v1.merge(v2);
            }
        });
        List<Tuple2<Tuple3<String, String, String>, Stats>> output = counts.collect();
        for (Tuple2<?, ?> t : output) {
            System.out.println("t._1()" + t._1() + "t._2() = " + t._2());
        }
        javaSparkContext.stop();

    }

    public static class Stats implements Serializable {
        private final int count;
        private final int numBytes;

        public Stats(int count, int numBytes) {
            this.count = count;
            this.numBytes = numBytes;
        }

        public Stats merge(Stats other) {
            return new Stats(count + other.count, numBytes + other.numBytes);
        }

        @Override
        public String toString() {
            return String.format("bytes= %s\tn=%s", numBytes, count);
        }
    }


}
