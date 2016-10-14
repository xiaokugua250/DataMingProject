package com.morty.java.dmp.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.elasticsearch.common.collect.Tuple;
import scala.Tuple2;

import java.util.List;

/**
 * Created by morty on 2016/05/25.
 */
public class SparkSimpleApp {
    public SparkConf sparkconf;
    public JavaSparkContext javaSparkContext;
    public Configuration conf;

//  /**
//   *
//   * @param javaRDD
//   * @param params
//   */
//  public void JavaRddComplexOpt(JavaRDD<?> javaRDD,String ... params){
//      // TODO: 2016/05/25  ����rdd����
//      javaRDD.filter(new Function<?, Boolean>() {
//          @Override
//          public Boolean call(Object v1) throws Exception {
//              return null;
//          }
//      });
//  }

    /**
     * @param javaRDD
     * @param params
     */
    // TODO: 2016/05/25  String JavaRDD ����demo
    // TODO: 2016/05/25 �μ�java api  http://spark.apache.org/docs/latest/api/java/index.html
    public void JavaComplexOpt(JavaRDD<String> javaRDD, String... params) {
        javaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.isEmpty();
            }
        });
        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                s.toLowerCase();
            }
        });
        javaRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {
                return new Tuple<String, Integer>(v1, 1);
            }
        });
        javaRDD.mapToPair(new PairFunction<String, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(String s) throws Exception {
                return new Tuple2<Object, Object>(s, 1);
            }
        });
        javaRDD.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + v2;
            }
        });
    }

    /**
     * ��ʼ���������ɸ���properties�ļ���xml�ļ���������
     */

    // TODO: 2016/05/25  ��ʼ��
    public void init() {

        //// TODO: 2016/05/25  ����sparkconf
        sparkconf = new SparkConf().setAppName("simpleApp");

        //// TODO: 2016/05/25  ����sparkcontext
        javaSparkContext = new JavaSparkContext(sparkconf);
        conf = new Configuration();
    }

    /**
     * RDD �������ڼ�rdd����
     *
     * @param javaRDDS
     * @param optType  ��������
     * @return
     */
    public JavaRDD<?> javaRDDSimpleOpt(String optType, JavaRDD<?>... javaRDDS) {

        // TODO: 2016/05/25  RDD ����

        /*
         * javaRDDS[0].cache();    //����
         * javaRDDS[1].distinct(); //ȥ��
         * javaRDDS[2].sample();   //ȡ��
         * ...
         *
         */
        return null;
    }

    /**
     * spark javardd action
     *
     * @param javaRdd
     * @param type
     * @param params
     */
    public void javaRddAct(JavaRDD<?> javaRdd, String type, String... params) {

        // TODO: 2016/05/25  rdd actions

        /*
         *  javaRdd.reduce();
         *
         * javaRdd.collect();
         *
         * javaRdd.count();
         *
         * javaRdd.first();
         *
         * javaRdd.take(5);
         *
         * javaRdd.takeSample();
         *
         * javaRdd.takeOrdered(5);
         *
         * javaRdd.saveAsTextFile("path");
         *
         * javaRdd.saveAsObjectFile("path");
         *
         * javaRdd.countByValue();
         *
         * javaRdd.foreach();
         */
    }

    /**
     * rdd ת�� �μ� http://spark.apache.org/docs/latest/api/java/index.html
     *
     * @param javaRdd
     * @param type
     * @param params
     */
    public JavaRDD<?> javaRddTrans(JavaRDD<?> javaRdd, String type, String... params) {

        // TODO: 2016/05/25  javardd ת������

        /*
         * javaRdd.map();
         *
         * javaRdd.filter();
         *
         * javaRdd.flatMap();
         *
         * javaRdd.mapPartitions();
         *
         * javaRdd.mapPartitionsWithIndex();
         *
         * javaRdd.sample();
         *
         * javaRdd.union();
         *
         * javaRdd.intersection();
         *
         * javaRdd.distinct();
         *
         * javaRdd.groupBy();
         *
         * javaRdd.reduce();
         *
         * javaRdd.aggregate();
         *
         * javaRdd.sortBy();
         *
         * javaRdd.coalesce();
         *
         * javaRdd.cartesian();
         *
         * javaRdd.repartition();
         */
        return null;
    }

    /**
     * RDD �־û�
     *
     * @param javaRdd
     * @param perisistType
     * @param params
     */
    public void rddPersistOpt(JavaRDD<?> javaRdd, String perisistType, String... params) {
        if (perisistType.equals("MENORY_ONLY")) {
            javaRdd.persist(StorageLevel.MEMORY_ONLY());
        } else if (perisistType.equals("MEMORY_AND_DISK")) {
            javaRdd.persist(StorageLevel.MEMORY_AND_DISK());
        } else if (perisistType.equals("DISK_ONLY")) {
            javaRdd.persist(StorageLevel.DISK_ONLY());
        } else {

            // Ĭ��Ϊmemory�洢
            javaRdd.cache();
        }
    }

    /**
     * @param javaRdd
     * @param params
     */
    public void rddUnPersistOpt(JavaRDD<?> javaRdd, String... params) {

        // TODO: 2016/05/25   ȡ�����ݻ���
        javaRdd.unpersist();
    }

    /**
     * �����ۼ���
     *
     * @param init
     * @return
     */
    public Accumulator<?> getAccumulators(Integer init) {
        return javaSparkContext.accumulator(init);
    }

    /**
     * �����㲥����
     *
     * @param broadValue
     * @return
     */
    public Broadcast<?> getBroadCastOpt(Object broadValue) {
        return javaSparkContext.broadcast(broadValue);
    }

    /**
     * �����㲥����
     *
     * @param broadValue
     * @return
     */
    public Broadcast<?> getBroadCastOpt(Object[] broadValue) {
        return javaSparkContext.broadcast(broadValue);
    }

    /**
     * ��ȡrdd ��hdfs��hive jdbc��������Դ
     *
     * @param fileName
     * @param params
     * @return
     */
    public JavaRDD<?> getJavaRDDComplex(String fileName, String... params) {

        // TODO: 2016/05/25  ��ȡSparkRdd
        JavaRDD<String> fileRdd = javaSparkContext.textFile(SparkInfo.HDFS_FILE);

        /*
         * JavaSparkContext.wholeTextFiles lets you read a directory containing multiple small text files,
         * and returns each of them as (filename, content) pairs.
         * This is in contrast with textFile, which would return one record per line in each file.
         */
        JavaPairRDD<String, String> dirRdd = javaSparkContext.wholeTextFiles("dir");

        /*
         * For SequenceFiles, use SparkContext��s sequenceFile[K, V] method where K and V are the types of key and values in the file.
         * These should be subclasses of Hadoop��s Writable interface, like IntWritable and Text.
         */
        JavaPairRDD<Text, Text> seqRdd = javaSparkContext.sequenceFile("path", Text.class, Text.class);

        // javaSparkContext.newAPIHadoopFile("path", Text.class,Text.class,IntWritable.class,conf);

        /*
         * javaSparkContext.hadoopFile();
         * javaSparkContext.hadoopRDD();
         * javaSparkContext.parallelize();
         * ...
         */
        return fileRdd;
    }

    /**
     * paralelize������ȡjavardd
     *
     * @param list
     * @param params
     * @return
     */
    public JavaRDD<?> getJavaRDDSimple(List<?> list, String... params) {

        // TODO: 2016/05/25  ��ȡ����list
        // List<String> data= Arrays.asList("1","2","3");
        return javaSparkContext.parallelize(list);
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
