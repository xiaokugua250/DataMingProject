package com.morty.java.dmp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by morty on 2016/05/16.
 * ģ����
 */
public class hadoopTmpl {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        /*
         *    GenericOptionsParser optionsParser=new GenericOptionsParser(conf,args);
         *  String[] remainingArgs=optionsParser.getRemainingArgs();
         *  if(!(remainingArgs.length != 2 || remainingArgs.length != 4)){
         *      System.err.println("usage:wordcount <in> <out> -[skip skipPatternFile]");
         *      System.exit(2);
         *  }
         */

        //// TODO: 2016/05/16  �������л���

        /*
         * org.apache.hadoop.mapreduce.Job job= org.apache.hadoop.mapreduce.Job.getInstance(conf,"word count");
         * job.setJarByClass(hadoopTmpl.class);
         * job.setMapperClass(Mapper.class);
         * job.setCombinerClass(Reducer.class);
         * job.setReducerClass(Reducer.class);
         * job.setOutputKeyClass(Text.class);
         * job.setOutputValueClass(IntWritable.class);
         *
         *
         * FileInputFormat.addInputPath(job,new Path(remainingArgs[0]));
         * FileOutputFormat.setOutputPath(job,new Path(remainingArgs[1]));
         * System.exit(job.waitForCompletion(true) ? 0 : 1);
         */
    }

    // todo map��
    public static class Map extends Mapper<ObjectWritable, ObjectWritable, ObjectWritable, ObjectWritable> {

        // todo map operation  map ҵ��
        @Override
        protected void map(ObjectWritable key, ObjectWritable value, Context context)
                throws IOException, InterruptedException {

            // super.map(key, value, context);
        }

        // todo setup ��ʼ������
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            // super.setup(context);
        }
    }


    // TODO  reduce ��
    public static class Reducer
            extends org.apache.hadoop.mapreduce.Reducer<ObjectWritable, ObjectWritable, ObjectWritable,
            ObjectWritable> {

        // todo clean up
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            // super.cleanup(context);
        }

        // todo Reduce ҵ��
        @Override
        protected void reduce(ObjectWritable key, Iterable<ObjectWritable> values, Context context)
                throws IOException, InterruptedException {

            // super.reduce(key, values, context);
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
