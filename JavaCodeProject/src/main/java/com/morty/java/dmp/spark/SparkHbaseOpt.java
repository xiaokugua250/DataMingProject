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

/** spark hbase 操作
 * 参见 http://wuchong.me/blog/2015/04/06/spark-on-hbase-new-api/
 * Created by morty on 2016/05/24.
 */
public class SparkHbaseOpt {

    public Configuration conf;

    public JavaSparkContext javaSparkContext;

    public SparkConf sparkConf;
    public Job job;     // mapreduce hadoop2.x jobconf -->job
    public Table table;
    public Connection connection;

    public void init() throws IOException {
        /*
        * 首先要向 HBase 写入数据，我们需要用到PairRDDFunctions.saveAsHadoopDataset。
        * 因为 HBase 不是一个文件系统，所以saveAsHadoopFile方法没用。
        def saveAsHadoopDataset(conf: JobConf): Unit
        Output the RDD to any Hadoop-supported storage system, using a Hadoop JobConf object for that storage system
        这个方法需要一个 JobConf 作为参数，类似于一个配置项，主要需要指定输出的格式和输出的表名。
        Step 1：我们需要先创建一个 JobConf。
        * */
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


    /* @param tuple3
     * @throws IOException
     */
    public void convert(Tuple3<Integer, String, String> tuple3) throws IOException {
        // TODO: 2016/05/24  方法存疑，注意检查

        /*
        * Step 2： RDD 到表模式的映射
        在 HBase 中的表 schema 一般是这样的：
        row     cf:col_1    cf:col_2
        而在Spark中，我们操作的是RDD元组，比如(1,"lilei",14), (2,"hanmei",18)。
        我们需要将 RDD[(uid:Int, name:String, age:Int)] 转换成 RDD[(ImmutableBytesWritable, Put)]。
        所以，我们定义一个 convert 函数做这个转换工作
                * */
        Put put = new Put(Bytes.toBytes(tuple3._1()));
        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_1), Bytes.toBytes(tuple3._2()));
        put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_2), Bytes.toBytes(tuple3._3()));

        new ImmutableBytesWritable(Bytes.toBytes(put.toString()));

        // table.put(put);  // TODO: 2016/05/25  检查这种方式可不可以

        /* scala 方式
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


    public void getRddConvert(JavaRDD<Tuple3<Integer, String, String>> dataRdd) {
        dataRdd.map(new Function<Tuple3<Integer, String, String>, Object>() {
            @Override
            public Object call(Tuple3<Integer, String, String> tuple3) throws Exception {
                Put put = new Put(Bytes.toBytes(tuple3._1()));
                put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_1), Bytes.toBytes(tuple3._2()));
                put.addColumn(Bytes.toBytes(SparkInfo.COLFAMILY), Bytes.toBytes(SparkInfo.COL_2), Bytes.toBytes(tuple3._3()));

                return new ImmutableBytesWritable(Bytes.toBytes(put.toString()));
            }
        });
    }
    /**
     * 保存数据到hbase
     *
     * @param
     */
    public void load2Hbase(JavaPairRDD<Tuple3<Integer, String, String>, String> javaPairRdd, JavaRDD javaRdd) {
        // TODO: 2016/05/25 方法存疑，需验证
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
}
