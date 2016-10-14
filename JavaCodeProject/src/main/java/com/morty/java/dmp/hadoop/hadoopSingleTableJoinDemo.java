package com.morty.java.dmp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by morty on 2016/05/16.
 */

/**
 * ʵ���и���child-parent�����ӡ�����ĸ����Ҫ�����grandchild-grandparent�����ӡ���ү�̣���
 * child        parent
 * Tom        Lucy
 * Tom        Jack
 * Jone        Lucy
 * Jone        Jack
 * Lucy        Mary
 * �������ʵ������Ȼ��Ҫ���е������ӣ����ӵ�������parent�к��ұ��child�У��������ұ���ͬһ����
 * ���ӽ���г�ȥ���ӵ����о�������Ҫ�Ľ������"grandchild--grandparent"��
 * Ҫ��MapReduce������ʵ��������Ӧ�ÿ������ʵ�ֱ�������ӣ���ξ��������е����ã�����ǽ��������
 * ���ǵ�MapReduce��shuffle���̻Ὣ��ͬ��key��������һ�����Կ��Խ�map�����key���óɴ����ӵ��У�Ȼ��������ͬ��ֵ����Ȼ��������һ���ˡ������ʼ�ķ�����ϵ������
 * Ҫ���ӵ�������parent�к��ұ��child�У��������ұ���ͬһ����������map�׶ν��������ݷָ��child��parent֮�󣬻Ὣparent���ó�key��child���ó�value�������������Ϊ����ٽ�ͬһ��child��parent�е�child���ó�key��parent���ó�value�����������Ϊ�ұ�Ϊ����������е����ұ���Ҫ�������value���ټ������ұ����Ϣ��������value��String�ʼ�������ַ�1��ʾ��������ַ�2��ʾ�ұ�������map�Ľ���о��γ��������ұ�Ȼ����shuffle������������ӡ�reduce���յ����ӵĽ��������ÿ��key��value-list�Ͱ�����"grandchild--grandparent"��ϵ��
 * ȡ��ÿ��key��value-list���н�����������е�child����һ�����飬�ұ��е�parent����һ�����飬Ȼ�������������ѿ������������Ľ����
 */
public class HadoopSingleTableJoinDemo {
    public static int time = 0;

    public static void main(String[] args) throws Exception {
        Configuration        conf          = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[]             remainingArgs = optionsParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.err.println("Usage: Single Table Join <in> <out>");
            System.exit(2);
        }

        org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf, "singleTableJoin");

        job.setJarByClass(HadoopSingleTableJoinDemo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        System.exit(job.waitForCompletion(true)
                    ? 0
                    : 1);
    }

    public static class Map extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String   line = value.toString();
            String[] str  = line.split("\t");

            context.write(new Text(str[1]), new Text("left+" + str[0]));
            context.write(new Text(str[0]), new Text("right+" + str[1]));
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // ��ͷ
            if (0 == time) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }

            String[] grandchild     = null;
            int      grandchildnum  = 0;
            String[] grandparent    = null;
            int      grandparentnum = 0;
            Iterator itr            = values.iterator();

            while (itr.hasNext()) {
                String   record = itr.next().toString();
                String[] st     = record.split("\\+");

                if (st[0].equals("left")) {
                    grandchild[grandchildnum] = st[1];
                    grandchildnum++;
                } else if (st[0].equals("right")) {
                    grandparent[grandparentnum] = st[1];
                    grandparentnum++;
                }
            }

            // grandchild��grandparent������ѿ�������
            if ((0 != grandchildnum) && (0 != grandparentnum)) {
                for (int m = 0; m < grandchildnum; m++) {
                    for (int n = 0; n < grandparentnum; n++) {
                        context.write(new Text(grandchild[m]), new Text(grandparent[n]));
                    }
                }
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
