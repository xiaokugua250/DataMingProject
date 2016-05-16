package com.morty.java.dmp.hadoop;

import java.io.FileReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by duliang on 2016/5/16.
 */
public class hadoopWordCountDemo {

   private static Logger LOG=Logger.getLogger(hadoopWordCountDemo.class);

    public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable> {
        static enum CounterEnum {INPUT_WORDS}
        private final static IntWritable one=new IntWritable();
        private Text word=new Text();
        private boolean caseSensitive;
        private Set<String> patternsToSkip=new HashSet<String>();
        private Configuration conf;
        private BufferedReader fis;

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
    }

}
