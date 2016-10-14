package com.morty.java.dmp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by morty on 2016/05/16.
 *
 * 1 ʵ������
 * �����������ļ���һ���������������������к͵�ַ����У���һ�������ַ��������ַ���к͵�ַ����С�Ҫ��������������ҳ��������͵�ַ���Ķ�Ӧ��ϵ�����"������������ַ��"��
 * ��������������ʾ��
 * 1��factory��
 * factoryname                ��������addressed
 * Beijing Red Star                ��������1
 * Shenzhen Thunder            ��������3
 * Guangzhou Honda            ��������2
 * Beijing Rising                   ��������1
 * Guangzhou Development Bank      2
 * Tencent                ����������������3
 * Back of Beijing                �������� 1
 * 2��address��
 * addressID    addressname
 * 1        ��������Beijing
 * 2        ��������Guangzhou
 * 3        ��������Shenzhen
 * 4        ��������Xian
 * �������������ʾ��
 * factoryname                    ��������addressname
 * Back of Beijing                    ��������  Beijing
 * Beijing Red Star                    ��������Beijing
 * Beijing Rising                    ���������� Beijing
 * Guangzhou Development Bank          Guangzhou
 * Guangzhou Honda                ��������Guangzhou
 * Shenzhen Thunder                ��������Shenzhen
 * Tencent                    ����������������Shenzhen
 * 2 ���˼·
 * �������͵���������ƣ������������ݿ��е���Ȼ���ӡ�
 * ��ȵ�������������������ұ�������и��������
 * ���Կ��Բ��ú͵����������ͬ�Ĵ���ʽ��mapʶ���������������ĸ���֮�󣬶�����зָ�����ӵ���ֵ������key�У���һ�к����ұ��ʶ������value�У�Ȼ�������
 * reduce�õ����ӽ��֮�󣬽���value���ݣ����ݱ�־�����ұ����ݷֿ���ţ�Ȼ����ѿ����������ֱ�������
 */
public class HadoopMutiTableJoinDemo {
    public static int time = 0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration        conf          = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[]             remainingArgs = optionsParser.getRemainingArgs();

        if (remainingArgs.length != 2) {
            System.err.println("Usage: Muti Table Join <in> <out>");
            System.exit(2);
        }

        org.apache.hadoop.mapreduce.Job job = org.apache.hadoop.mapreduce.Job.getInstance(conf, "MutiTableJoin");

        job.setJarByClass(HadoopMutiTableJoinDemo.class);
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
            String line         = value.toString();
            String relationType = new String();

            if ((line.contains("factoryName") == true) || (line.contains("addressID") == true)) {
                return;
            }

            StringTokenizer itr      = new StringTokenizer(line);
            String          mapKey   = new String();
            String          mapValue = new String();
            String[]        split    = line.split(" ");

            if ((line.length() == 2) && (split[1].charAt(0) >= '0') && (split[1].charAt(0) <= '9')) {
                mapKey       = split[1];
                mapValue     = split[0];
                relationType = "1";
            }

            if ((line.length() == 2) && (split[0].charAt(0) >= '0') && (split[0].charAt(0) <= '9')) {
                mapKey       = split[0];
                mapValue     = split[1];
                relationType = "2";
            }

            context.write(new Text(mapKey), new Text(relationType + "+" + mapValue));
        }
    }


    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (0 == time) {
                context.write(new Text("Factoryname"), new Text("Adressname"));
                time++;
            }

            int      factorynum = 0;
            String[] factory    = new String[10];
            int      addressnum = 0;
            String[] address    = new String[10];

            for (Text value : values) {
                if (0 == value.toString().length()) {
                    continue;
                }

                char relationType = value.toString().charAt(0);

                // left
                if ('1' == relationType) {
                    factory[factorynum] = value.toString().substring(2);
                    factorynum++;
                }

                // right
                if ('2' == relationType) {
                    address[addressnum] = value.toString().substring(2);
                    addressnum++;
                }
            }

            if ((0 != factorynum) && (0 != addressnum)) {
                for (int m = 0; m < factorynum; m++) {
                    for (int n = 0; n < addressnum; n++) {
                        context.write(new Text(factory[m]), new Text(address[n]));
                    }
                }
            }
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
