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
public class HadoopDedupicateDemo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // ��ȡ�����ļ�������ļ��ĵ�ַ
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage:  in and out");
            System.exit(2);
        }

        Job job = new Job(conf, "Data deduplication");

        job.setJarByClass(HadoopDedupicateDemo.class);
        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true)
                    ? 0
                    : 1);
    }

    /*
     * 1,Map�������е�value���Ƶ�������ݵ�key�ϣ���ֱ�����,valueֵ����ν������shuffle������̽���key��ͬ����
     */
    public static class Map extends Mapper<Object, Text, Text, Text> {
        private static Text line = new Text();

        // map����ֱ�ӽ�value���Ƹ�line��Ȼ���������
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            line = value;
            context.write(line, new Text(""));
        }
    }


    /*
     * 2,reduce�������key���Ƶ�������ݵ�key�ϣ���ֱ�����
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        // reduce����������shuffle����õģ�ֱ��������ɣ��Ƚϼ�
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
