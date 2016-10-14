package com.morty.java.dmp.hadoopElasticsearch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by morty on 2016/10/14.
 */
public class HadoopElsticConfig {

    private static final String ES_SERVER = "es-server:9200";
    private static final String ES_RESOURCE = "radio/artists";
    private Configuration configuration;
    private Job job;

    public void init() {
        configuration = new Configuration();
        configuration.set("es.nodes", ES_SERVER);
        configuration.set("es.resource", ES_RESOURCE);
    }

    public void initForHadoop() {
        configuration = new Configuration();
        configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
        configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        configuration.set("es.node", ES_SERVER);
        configuration.set("es.resource", ES_RESOURCE);
    }

    /**
     * Writing to dynamic/multi-resources
     */
    public void initForHadoopWrite() {
        configuration = new Configuration();
        configuration.set("es.resource.write", "my-collection/{media-type}");
    }

    /**
     * writing data to es
     */
    public void initHadoopJobForESWrite() {
        try {
            job = new Job(configuration);
            job.setOutputFormatClass(EsOutputFormat.class);
            job.setMapOutputValueClass(MapWritable.class);
            //job.xx
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    /**
     * reading data from es
     */
    public void initHadoopJobForESRead() {
        try {
            job = new Job(configuration);
            job.setInputFormatClass(EsInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(MapWritable.class);
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * write doc to es
     * Writing to dynamic/multi-resources
     */
    public class EsWriteMapperWrite extends Mapper {

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            MapWritable doc = new MapWritable();

            //// TODO: 2016/10/14
            context.write(NullWritable.get(), doc);
        }
    }

    public class EsWriteMapperJson extends Mapper {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            //  // assuming the document is stored as bytes
            //   byte[] source = value.toString().getBytes() ;//....;
            // TODO: 2016/10/14    byte[] source = ... ;
            //for example
            byte[] source = value.toString().getBytes();
            BytesWritable jsonDoc = new BytesWritable(source);
            context.write(NullWritable.get(), jsonDoc);

        }
    }

    public class EsReadMapper extends Mapper {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            Text docId = (Text) key;
            MapWritable doc = (MapWritable) value;
        }
    }

}
