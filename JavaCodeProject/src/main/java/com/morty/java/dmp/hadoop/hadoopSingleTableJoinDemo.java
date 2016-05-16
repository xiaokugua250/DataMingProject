package com.morty.java.dmp.hadoop;

/**
 * Created by morty on 2016/05/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * 实例中给出child-parent（孩子——父母）表，要求输出grandchild-grandparent（孙子——爷奶）表。
 * child        parent
 Tom        Lucy
 Tom        Jack
 Jone        Lucy
 Jone        Jack
 Lucy        Mary
 分析这个实例，显然需要进行单表连接，连接的是左表的parent列和右表的child列，且左表和右表是同一个表。
连接结果中除去连接的两列就是所需要的结果——"grandchild--grandparent"表。
 要用MapReduce解决这个实例，首先应该考虑如何实现表的自连接；其次就是连接列的设置；最后是结果的整理。
 考虑到MapReduce的shuffle过程会将相同的key会连接在一起，所以可以将map结果的key设置成待连接的列，然后列中相同的值就自然会连接在一起了。再与最开始的分析联系起来：
 要连接的是左表的parent列和右表的child列，且左表和右表是同一个表，所以在map阶段将读入数据分割成child和parent之后，会将parent设置成key，child设置成value进行输出，并作为左表；再将同一对child和parent中的child设置成key，parent设置成value进行输出，作为右表。为了区分输出中的左右表，需要在输出的value中再加上左右表的信息，比如在value的String最开始处加上字符1表示左表，加上字符2表示右表。这样在map的结果中就形成了左表和右表，然后在shuffle过程中完成连接。reduce接收到连接的结果，其中每个key的value-list就包含了"grandchild--grandparent"关系。
 取出每个key的value-list进行解析，将左表中的child放入一个数组，右表中的parent放入一个数组，然后对两个数组求笛卡尔积就是最后的结果了
 */
public class hadoopSingleTableJoinDemo {
    public static int time=0;

    public static class Map extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String[] str=line.split("\t");
            context.write(new Text(str[1]),new Text("left+"+str[0]));
            context.write(new Text(str[0]),new Text("right+"+str[1]));
        }
    }

    public static  class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //表头
            if(0 == time){
                context.write(new Text("grandchild"),new Text("grandparent"));
                time++;
            }
            String[] grandchild=null;
            int grandchildnum=0;
            String[] grandparent=null;
            int grandparentnum=0;
            Iterator itr=values.iterator();
            while (itr.hasNext()){
                String record=itr.next().toString();
                String[] st=record.split("\\+");
                if(st[0].equals("left")){
                    grandchild[grandchildnum]=st[1];
                    grandchildnum++;
                }else if(st[0].equals("right")) {
                    grandparent[grandparentnum]=st[1];
                    grandparentnum++;
                }
            }
            //grandchild和grandparent数组求笛卡尔儿积
            if( 0 != grandchildnum && 0 != grandparentnum){
                for(int m=0;m<grandchildnum;m++){
                    for(int n=0;n<grandparentnum;n++){
                        context.write(new Text(grandchild[m]),new Text(grandparent[n]));
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws  Exception {
        Configuration conf=new Configuration();
        GenericOptionsParser optionsParser=new GenericOptionsParser(conf,args);
        String[] remainingArgs=optionsParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: Single Table Join <in> <out>");
            System.exit(2);
        }
        org.apache.hadoop.mapreduce.Job job= org.apache.hadoop.mapreduce.Job.getInstance(conf,"singleTableJoin");
        job.setJarByClass(hadoopSingleTableJoinDemo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(remainingArgs[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,new Path(remainingArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 :1);
    }

}
