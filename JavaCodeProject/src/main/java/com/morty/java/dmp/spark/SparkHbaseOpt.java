package com.morty.java.dmp.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple3;

import java.io.IOException;

/**
 * spark hbase ����
 * �μ� http://wuchong.me/blog/2015/04/06/spark-on-hbase-new-api
 * Created by morty on 2016/05/24.
 */
public class SparkHbaseOpt {
    public Configuration conf;
    public JavaSparkContext javaSparkContext;
    public SparkConf sparkConf;
    public Job job;    // mapreduce hadoop2.x jobconf -->job
    public Table table;
    public Connection connection;

    /*
     *  @param tuple3
     * @throws IOException
     */
    public void convert(Tuple3<Integer, String, String> tuple3) throws IOException {

        // TODO: 2016/05/24  �������ɣ�ע����

        /*
         * Step 2�� RDD ����ģʽ��ӳ��
         * �� HBase �еı� schema һ���������ģ�
         * row     cf:col_1    cf:col_2
         * ����Spark�У����ǲ�������RDDԪ�飬����(1,"lilei",14), (2,"hanmei",18)��
         * ������Ҫ�� RDD[(uid:Int, name:String, age:Int)] ת���� RDD[(ImmutableBytesWritable, Put)]��
         * ���ԣ����Ƕ���һ�� convert ���������ת������
         */
        Put put = new Put(Bytes.toBytes(tuple3._1()));

        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_1), Bytes.toBytes(tuple3._2()));
        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_2), Bytes.toBytes(tuple3._3()));
        new ImmutableBytesWritable(Bytes.toBytes(put.toString()));

        // table.put(put);  // TODO: 2016/05/25  ������ַ�ʽ�ɲ�����

        /*
         *  scala ��ʽ
         * def convert(triple: (Int, Int, Int)) = {
         * val p = new Put(Bytes.toBytes(triple._1))
         * p.add(Bytes.toBytes("cf"),
         *   Bytes.toBytes("col_1"),
         *   Bytes.toBytes(triple._2))
         * p.add(Bytes.toBytes("cf"),
         *   Bytes.toBytes("col_2"),
         *   Bytes.toBytes(triple._3))
         * (new ImmutableBytesWritable, p)
         * }
         */
    }

    public void init() throws IOException {

        /*
         * ����Ҫ�� HBase д�����ݣ�������Ҫ�õ�PairRDDFunctions.saveAsHadoopDataset��
         * ��Ϊ HBase ����һ���ļ�ϵͳ������saveAsHadoopFile����û�á�
         * def saveAsHadoopDataset(conf: JobConf): Unit
         * Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for that storage system
         * ���������Ҫһ�� JobConf ��Ϊ������������һ���������Ҫ��Ҫָ������ĸ�ʽ������ı�����
         * Step 1��������Ҫ�ȴ���һ�� JobConf��
         */
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", SparkInfo.HBASE_ROOTDIR);
        conf.setBoolean("hbase.cluster.distributed", true);
        conf.set("hbase.zookeeper.quorum", SparkInfo.ZKQUORUM);
        conf.setInt("hbase.client.scanner.caching", 10000);

        // ....some other settings
        job = Job.getInstance(conf);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, SparkInfo.HBASEOUTTABLE);
        table = connection.getTable(TableName.valueOf(SparkInfo.HBASEOUTTABLE));

        /*
         *  // Default HBase configuration for connecting to localhost on default port.
         * conf = HBaseConfiguration.create();
         * // Simple Spark configuration where everything runs in process using 2 worker threads.
         * sparkConf = new SparkConf().setAppName("Jail With Spark").setMaster("local[2]");
         * // The Java Spark context provides Java-friendly interface for working with Spark RDDs
         * javaSparkContext = new JavaSparkContext(sparkConf);
         * // The HBase context for Spark offers general purpose method for bulk read/write
         * hbaseContext = new JavaHBaseContext(javaSparkContext, conf);
         * // The entry point interface for the Spark SQL processing module. SQL-like data frames
         * // can be created from it.
         * sqlContext = new org.apache.spark.sql.SQLContext(jsc);
         */
    }

    /**
     * �������ݵ�hbase
     *
     * @param
     */
    public void load2Hbase(JavaPairRDD<Tuple3<Integer, String, String>, String> javaPairRdd, JavaRDD javaRdd) {

        // TODO: 2016/05/25 �������ɣ�����֤
        javaPairRdd.saveAsNewAPIHadoopDataset(conf);

/*
            javaPairRdd=javaRdd.mapToPair(new PairFunction() {
                @Override
                public Tuple2 call(Object o) throws Exception {
                    return null;
                }
            });
            javaPairRdd.saveAsNewAPIHadoopDataset(conf);
        }
*/
    }

    public void getRddConvert(JavaRDD<Tuple3<Integer, String, String>> dataRdd) {
        dataRdd.map(new Function<Tuple3<Integer, String, String>, Object>() {
            @Override
            public Object call(Tuple3<Integer, String, String> tuple3) throws Exception {
                Put put = new Put(Bytes.toBytes(tuple3._1()));

                put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY),
                        Bytes.toBytes(SparkInfo.COL_1),
                        Bytes.toBytes(tuple3._2()));
                put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY),
                        Bytes.toBytes(SparkInfo.COL_2),
                        Bytes.toBytes(tuple3._3()));

                return new ImmutableBytesWritable(Bytes.toBytes(put.toString()));
            }
        });
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
