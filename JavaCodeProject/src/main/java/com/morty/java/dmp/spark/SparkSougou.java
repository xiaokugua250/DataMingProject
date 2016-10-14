package com.morty.java.dmp.spark;

/**
 * Created by duliang on 2016/6/25.
 */
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * �ѹ���־����
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/25
 * Time: 10:23
 * email:duliang1128@163.com
 */
public class SparkSougou {
    JavaSparkContext javaSparkContext = null;
    SparkConf sparkConf = null;

    public void init() {
        sparkConf = new SparkConf();
        sparkConf.setAppName("sougouLog_duLiang")
                .setMaster("dev4")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.cores", "4")
                .set("spark.eventLog.enabled", "true");
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    public void sogouLogAna() {

        // TODO: 2016/6/25 Ҫ���
        JavaRDD<String> sogouLog = javaSparkContext.textFile("/duliang/warehouse/spark/sougou/SogouQ.reduced");

        /*
         * JavaRDD<String> filter = (JavaRDD<String>) sogouLog.map(new Function<String, String[]>() {
         *   @Override
         *   public String[] call(String s) throws Exception {
         *       return s.split("\\t");
         *   }
         * }).filter(new Function<String[], Boolean>() {
         *   @Override
         *   public Boolean call(String[] log) throws Exception {
         *       return log[6].length() == 6;
         *   }
         *
         *
         * });
         */
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
