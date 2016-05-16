package com.morty.java.dmp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by morty on 2016/05/16.
 */
public class hadoopDedupicateDemo {
    /*
    * 1,Map将输入中的value复制到输出数据的key上，并直接输出,value值无所谓，利用shuffle这个工程进行key相同汇总
    */
    public static class Map extends Mapper<Object,Text,Text,Text>{
        private static Text line=new Text();

        // map函数直接将value复制给line，然后输出即可
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line=value;
            context.write(line,new Text(""));
        }
    }
    /*
   * 2,reduce将输入的key复制到输出数据的key上，并直接输出
   */
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        // reduce函数，利用shuffle处理好的，直接输出即可，比较简单

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key,new Text(""));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 获取输入文件和输出文件的地址
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage:  in and out");
            System.exit(2);
        }

        Job job = new Job(conf, "Data deduplication");
        job.setJarByClass(hadoopDedupicateDemo.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
