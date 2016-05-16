package com.morty.java.dmp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by morty on 2016/05/16.
 * 模板类
 */
public class hadoopTmpl {

    //todo map类
    public static class Map extends Mapper<ObjectWritable,ObjectWritable,ObjectWritable,ObjectWritable>{

        // todo setup 初始化方法
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            //super.setup(context);
        }


        //todo map operation  map 业务
        @Override
        protected void map(ObjectWritable key, ObjectWritable value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
        }
    }

    //TODO  reduce 类
    public static class Reducer extends org.apache.hadoop.mapreduce.Reducer<ObjectWritable,ObjectWritable,ObjectWritable,ObjectWritable>{

        //todo Reduce 业务
        @Override
        protected void reduce(ObjectWritable key, Iterable<ObjectWritable> values, Context context) throws IOException, InterruptedException {
            //super.reduce(key, values, context);
        }

        //todo clean up
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //super.cleanup(context);
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf=new Configuration();
     /*   GenericOptionsParser optionsParser=new GenericOptionsParser(conf,args);
        String[] remainingArgs=optionsParser.getRemainingArgs();
        if(!(remainingArgs.length != 2 || remainingArgs.length != 4)){
            System.err.println("usage:wordcount <in> <out> -[skip skipPatternFile]");
            System.exit(2);
        }*/
        //// TODO: 2016/05/16  设置运行环境

        /*org.apache.hadoop.mapreduce.Job job= org.apache.hadoop.mapreduce.Job.getInstance(conf,"word count");
        job.setJarByClass(hadoopTmpl.class);
        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Reducer.class);
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job,new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job,new Path(remainingArgs[1]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
        */

    }
}
