package com.morty.java.dmp.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple3;

import java.io.IOException;

/**
 * Created by morty on 2016/05/24.
 */
public class SparkLoadDataHbase {

    public Configuration conf;

    public JavaSparkContext javaSparkContext;

    public SparkConf sparkConf;
    public Job job;     // mapreduce hadoop2.x jobconf -->job
    public Table table;
    public Connection connection;

    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", SparkInfo.HBASE_ROOTDIR);
        conf.setBoolean("hbase.cluster.distributed", true);
        conf.set("hbase.zookeeper.quorum", SparkInfo.ZKQUORUM);
        conf.setInt("hbase.client.scanner.caching", 10000);
        //....some other settings

        job = Job.getInstance(conf);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, SparkInfo.HBASEOUTTABLE);
        table = connection.getTable(TableName.valueOf(SparkInfo.HBASEOUTTABLE));
       /* // Default HBase configuration for connecting to localhost on default port.
        conf = HBaseConfiguration.create();
        // Simple Spark configuration where everything runs in process using 2 worker threads.
        sparkConf = new SparkConf().setAppName("Jail With Spark").setMaster("local[2]");
        // The Java Spark context provides Java-friendly interface for working with Spark RDDs
        javaSparkContext = new JavaSparkContext(sparkConf);
        // The HBase context for Spark offers general purpose method for bulk read/write
        hbaseContext = new JavaHBaseContext(javaSparkContext, conf);
        // The entry point interface for the Spark SQL processing module. SQL-like data frames
        // can be created from it.
        sqlContext = new org.apache.spark.sql.SQLContext(jsc);*/

    }


    /**
     * @param tuple3
     * @throws IOException
     */
    public void insertColunm(Tuple3<Integer, Integer, Integer> tuple3) throws IOException {
        Put put = new Put(Bytes.toBytes(tuple3._1()));
        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_1), Bytes.toBytes(tuple3._2()));
        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_2), Bytes.toBytes(tuple3._3()));

        //// TODO: 2016/05/24  方法存疑，注意检查
        //new ImmutableBytesWritable(Bytes.toBytes(put.toString()));

        table.put(put);
        /*
        * def convert(triple: (Int, Int, Int)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.add(Bytes.toBytes("cf"),
            Bytes.toBytes("col_1"),
            Bytes.toBytes(triple._2))
      p.add(Bytes.toBytes("cf"),
            Bytes.toBytes("col_2"),
            Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, p)
}
        * */


    }


    /**
     * 保存数据到hbase
     *
     * @param rddData
     */
    public void load2Hbase(JavaRDD<Tuple3<Integer, Integer, Integer>> rddData) {

        // new PairRDDFunctions<>(rddData.map(loadDataFormat)).saveAsHadoopDataset(conf);
    }

}
