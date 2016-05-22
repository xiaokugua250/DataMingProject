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
 * 1 实例描述
 输入是两个文件，一个代表工厂表，包含工厂名列和地址编号列；另一个代表地址表，包含地址名列和地址编号列。要求从输入数据中找出工厂名和地址名的对应关系，输出"工厂名——地址名"表。
 样例输入如下所示。
 1）factory：
 factoryname                　　　　addressed
 Beijing Red Star                　　　　1
 Shenzhen Thunder            　　　　3
 Guangzhou Honda            　　　　2
 Beijing Rising                   　　　　1
 Guangzhou Development Bank      2
 Tencent                　　　　　　　　3
 Back of Beijing                　　　　 1
 2）address：
 addressID    addressname
 1        　　　　Beijing
 2        　　　　Guangzhou
 3        　　　　Shenzhen
 4        　　　　Xian
 样例输出如下所示。
 factoryname                    　　　　addressname
 Back of Beijing                    　　　　  Beijing
 Beijing Red Star                    　　　　Beijing
 Beijing Rising                    　　　　　 Beijing
 Guangzhou Development Bank          Guangzhou
 Guangzhou Honda                　　　　Guangzhou
 Shenzhen Thunder                　　　　Shenzhen
 Tencent                    　　　　　　　　Shenzhen
 2 设计思路
 多表关联和单表关联相似，都类似于数据库中的自然连接。
 相比单表关联，多表关联的左右表和连接列更加清楚。
 所以可以采用和单表关联的相同的处理方式，map识别出输入的行属于哪个表之后，对其进行分割，将连接的列值保存在key中，另一列和左右表标识保存在value中，然后输出。
 reduce拿到连接结果之后，解析value内容，根据标志将左右表内容分开存放，然后求笛卡尔积，最后直接输出。
 */
public class HadoopMutiTableJoinDemo {
    public static int time=0;

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf=new Configuration();
        GenericOptionsParser optionsParser=new GenericOptionsParser(conf,args);
        String[] remainingArgs=optionsParser.getRemainingArgs();
        if (remainingArgs.length != 2) {
            System.err.println("Usage: Muti Table Join <in> <out>");
            System.exit(2);
        }
        org.apache.hadoop.mapreduce.Job job= org.apache.hadoop.mapreduce.Job.getInstance(conf,"MutiTableJoin");
        job.setJarByClass(HadoopMutiTableJoinDemo.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job,new Path(remainingArgs[0]));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job,new Path(remainingArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 :1);
    }

    public static class Map extends Mapper<Object,Text,Text,Text>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String relationType=new String();
            if(line.contains("factoryName") == true || line.contains("addressID") ==  true){
                return;
            }
            StringTokenizer itr=new StringTokenizer(line);
            String mapKey=new String ();
            String mapValue=new String();
            String[] split=line.split(" ");
            if(line.length() ==2 && split[1].charAt(0) >= '0' && split[1].charAt(0) <= '9'){
                mapKey=split[1];
                mapValue=split[0];
                relationType="1";
            }
            if(line.length() ==2 && split[0].charAt(0) >= '0' && split[0].charAt(0) <= '9'){
                mapKey=split[0];
                mapValue=split[1];
                relationType="2";
            }
            context.write(new Text(mapKey),new Text(relationType+"+"+mapValue));

        }
    }

    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if(0 ==time){
                context.write(new Text("Factoryname"),new Text("Adressname") );
                time++;
            }
            int factorynum=0;
            String[] factory=new String[10];
            int addressnum=0;
            String[] address=new String[10];
            for(Text value:values){
                if(0 == value.toString().length()){
                    continue;
                }
                char relationType=value.toString().charAt(0);
                //left
                if( '1' == relationType){
                    factory[factorynum]=value.toString().substring(2);
                    factorynum++;
                }
                //right
                if('2' == relationType){
                    address[addressnum]=value.toString().substring(2);
                    addressnum++;
                }
            }

            if( 0 != factorynum && 0 != addressnum){
                for(int m=0;m < factorynum ; m++){
                    for(int n=0;n < addressnum;n++){
                        context.write(new Text(factory[m]),new Text(address[n]));
                    }
                }
            }
        }
    }
}
