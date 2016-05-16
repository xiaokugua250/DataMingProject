package com.morty.java.dmp.hadoop;

import java.io.FileReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;


/**
 * Created by duliang on 2016/5/16.
 */
public class hadoopWordCountDemo {

   private static Logger LOG=Logger.getLogger(hadoopWordCountDemo.class);

    // Map class
    public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable> {
        static enum CounterEnum {INPUT_WORDS}
        private final static IntWritable one=new IntWritable();
        private Text word=new Text();
        private boolean caseSensitive;
        private Set<String> patternsToSkip=new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;
        
        //// TODO: 2016/05/16  setup init 
        //setup
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            conf=context.getConfiguration();
            caseSensitive=conf.getBoolean("wordcount.case.sensitive",true);
            if(conf.getBoolean("wordcount.skip.patterns",true)){
                URI[] patternsURIs= org.apache.hadoop.mapreduce.Job.getInstance(conf).getCacheFiles();
                for(URI patternsURI:patternsURIs){
                    Path patternPath=new Path(patternsURI.getPath());
                    String patternsFileName=patternPath.getName().toString();
                    parseSkipFile(patternsFileName);
                }
            }
        }
        
        private void parseSkipFile(String patternsFileName) {
            try{
                fis=new BufferedReader(new FileReader(patternsFileName));
                String pattern=null;
                while ((pattern = fis.readLine()) != null){
                    patternsToSkip.add(pattern);
                }
            }catch (IOException e){
                e.printStackTrace();
                System.err.println("Caught exception while parsing the cached file '"
                        + StringUtils.stringifyException(e));
                LOG.error("Caught exception while parsing the cached file "+e.getMessage());
            }
        }

        
        //// TODO: 2016/05/16  map operation 
        //Map Operation
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line=(caseSensitive) ? value.toString() : value.toString().toLowerCase();
            for(String pattern:patternsToSkip){
                line=line.replaceAll(pattern,"");
            }
            StringTokenizer itr=new StringTokenizer(line);
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
                Counter counter=context.getCounter(CounterEnum.class.getName(),CounterEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }
    
    
    //// TODO: 2016/05/16  reduce operation 
    //Reducer class
    public static class IntSumReducer extends org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result=new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable val : values){
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }

    }

    
    // main class for job control
    public static void main(String[] args)  throws Exception{
        Configuration conf=new Configuration();
        GenericOptionsParser optionsParser=new GenericOptionsParser(conf,args);
        String[] remainingArgs=optionsParser.getRemainingArgs();
        if(!(remainingArgs.length != 2 || remainingArgs.length != 4)){
            System.err.println("usage:wordcount <in> <out> -[skip skipPatternFile]");
            System.exit(2);
        }
        org.apache.hadoop.mapreduce.Job job= org.apache.hadoop.mapreduce.Job.getInstance(conf,"word count");
        job.setJarByClass(hadoopWordCountDemo.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs=new ArrayList<String>();
        for(int i=0;i<remainingArgs.length;i++){
            if("-skip".equals(remainingArgs[i])){
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns",true);
            }else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job,new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job,new Path(otherArgs.get(1)));


        //job.waitForCompletion(true);
    }

}
