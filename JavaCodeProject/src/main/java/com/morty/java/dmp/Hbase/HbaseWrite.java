package com.morty.java.dmp.Hbase;
/**
 * Created by duliang on 2016/7/27.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/7/27
 * Time: 9:35
 * email:duliang1128@163.com
 */
public class HbaseWrite extends Configured implements Tool {
    static final Log LoG = LogFactory.getLog(HbaseWrite.class);

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // TODO: 2016/7/28      String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        int res = 1;
        try {
            // TODO: 2016/7/28        res= ToolRunner.run(conf,new HbaseWrite(),otherArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(1);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            LoG.info("usage:3 params needed !\n hadoop jar hbase-build-import-0.1.jar\"" +
                    "<inputpath><tablename><columns>");
            System.exit(1);
        }

        String input = args[0];
        String table = args[1];
        String columns = args[2];

        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "dev1:60000");
        conf.set("hbase.zookeeper.quorum", "http://dev1,http://dev2");
        Job job = new Job(conf, "Import from file" + input + "into table" + table);
        job.setJarByClass(HbaseWrite.class);
        job.setMapperClass(HbaseWriteMapper.class);

        // TODO: 2016/7/28  job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, table);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(input));
        return job.waitForCompletion(true) ? 0 : 1;


    }

    public class HbaseWriteMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Writable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineString = value.toString();
            String[] arr = lineString.split("\t", -1);
            try {
                if (arr.length == 2) {
                    String rowkey = arr[0];
                    String[] vals = arr[1].split("\u0001", -1);
                    if (vals.length == 4) {
                        byte[] family = vals[0].getBytes();
                        byte[] qualifier = vals[1].getBytes();
                        byte[] val = vals[2].getBytes();
                        Long ts = Long.parseLong(vals[3]);
                        Put put = new Put(rowkey.getBytes(), ts);
                        put.add(family, qualifier, val);
                        // TODO: 2016/7/28  context.write(new ImmutableBytesWritable(rowkey.getBytes()),put);
                    }
                }
            } catch (NumberFormatException e) {
                e.printStackTrace();

            }
        }
    }
}
