package com.morty.java.dmp.Hbase;
/**
 * Created by duliang on 2016/7/25.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/7/25
 * Time: 23:00
 * email:duliang1128@163.com
 */
public class HbaseRead {
    // 读取输入源Hbase
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf, "ExampleRead");
        // TODO: 2016/7/28   job.setJarByClass(ReadHbaseJob.class);
        // TODO: 2016/7/25  class that contains mapper

        Scan scan = new Scan();
        scan.setBatch(0);
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setMaxVersions();
        scan.setTimeRange(System.currentTimeMillis() - 3 * 24 * 3600 * 1000L, System.currentTimeMillis());
        /* configure scan */
        scan.addColumn(Bytes.toBytes("family1"), Bytes.toBytes("qulifier1"));
        conf.set("hbase.master", "dev1:60000");
        conf.set("hbase.zookeeper.quorum", "http://dev1,http://dev2");


        TableMapReduceUtil.initTableMapperJob(
                "tableName",        // input HBase table name
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper
                null,             // mapper output key
                null,             // mapper output value
                job
        );
        job.setOutputFormatClass(NullOutputFormat.class);  // because we aren't emitting anything from mapper
        Boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job");
        }

    }

    public static class MyMapper extends TableMapper<Text, Text> {

        public static final String FIELD_COMMON_SEPARATOR = "\u0001";
        private Text k = new Text();
        private Text v = new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result Columns, Context context) throws IOException, InterruptedException {
            // TODO: 2016/7/25   // process data for the row from the Result instance.
            String value_1 = null;

            String rowkey = new String(key.get());
            byte[] columFamily = null;
            byte[] columnQualifier = null;
            long ts = 0L;
            try {
                for (KeyValue kv : Columns.list()) {
                    value_1 = Bytes.toStringBinary(kv.getValue());
                    columFamily = kv.getFamily();
                    columnQualifier = kv.getQualifier();
                    ts = kv.getTimestamp();


                    if ("value1".equals(value_1)) {
                        k.set(rowkey);
                        v.set(Bytes.toString(columFamily) + FIELD_COMMON_SEPARATOR
                                + Bytes.toString(columnQualifier)
                                + FIELD_COMMON_SEPARATOR + value_1 +
                                FIELD_COMMON_SEPARATOR + ts);
                        context.write(k, v);
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
