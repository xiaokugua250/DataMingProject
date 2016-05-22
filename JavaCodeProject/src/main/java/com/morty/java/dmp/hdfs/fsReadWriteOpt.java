package com.morty.java.dmp.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import static com.morty.java.dmp.hdfs.FSInfo.DATA_SOURCE_URI;
import static org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;

/**
 * Created by duliang on 2016/5/13.
 */
public class FSReadWriteOpt {

    public Configuration conf;
    FileSystem fileSystem;
    InputStream inputStream=null;

    public void init(){
        conf=new Configuration();
        try {
            fileSystem=FileSystem.get(URI.create(DATA_SOURCE_URI),conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取fs
     * @param dataUri
     * @return
     */
    public FileSystem getFileSystem(String dataUri){
        try {
            fileSystem=FileSystem.get(URI.create(dataUri),conf);
            return fileSystem;

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 显示hdfs文件内容
     * @param dataUri
     */
    public void fsCat(String dataUri){
        try {
            inputStream=fileSystem.open(new Path(dataUri));
            IOUtils.copyBytes(inputStream,System.out,4096,false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }

    }


    /**
     * 文件复制
     * @param source
     * @param destination
     */
    public void fsCopy(String source,String destination){

        try {

            InputStream in=new BufferedInputStream(new FileInputStream(source));
            OutputStream outputStream=fileSystem.create(new Path(destination));
            IOUtils.copyBytes(in,outputStream,4096,false);

        } catch (IOException e) {

            e.printStackTrace();
        }
    }


    /**
     * 获取文件块信息
     * @param source
     * @return
     */
    public Object getfSLocation(String source){

        try {
            FileStatus fileStatus=fileSystem.getFileStatus(new Path(source));
            BlockLocation[]  blockLocations=fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());

            /*for(BlockLocation blockLocation: blockLocations){
                String[] cachedHosts= blockLocation.getCachedHosts();
                String[] hosts=blockLocation.getHosts();
                String[] names=blockLocation.getNames();
                long lens=blockLocation.getLength();
            }*/

            return  blockLocations;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }


    /**
     *
     * @param text
     * @param destinaTion
     */
    public void writSeqFile(Object[] text,String destinaTion){
        SequenceFile.Writer writer=null;

        IntWritable key=new IntWritable();
        Text value=new Text();

        List<SequenceFile.Writer.Option> options=new LinkedList<SequenceFile.Writer.Option>();
        SequenceFile.Writer.Option pathOpt=SequenceFile.Writer.file(new Path(destinaTion));
        SequenceFile.Writer.Option keyOpt=SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valueOpt=SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option ComOpt=SequenceFile.Writer.compression(BLOCK);
        options.add(pathOpt);
        options.add(keyOpt);
        options.add(valueOpt);
        options.add(ComOpt);

        SequenceFile.Writer.Option[] opts= (SequenceFile.Writer.Option[]) options.toArray();

        //conf.addResource(new Path(destinaTion));

        try {
            writer= SequenceFile.createWriter(conf, opts);
            //todo  写入内容与逻辑
            for(int i=0;i<=100;i++){
                key.set(100-i);
                value.set((byte[]) text[i%text.length]);
                writer.append(key,value);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(writer);
        }

    }


    /**
     *
     * @param text
     * @param destinaTion
     */
    public void readSeqFile(Object[] text,String destinaTion){
        SequenceFile.Reader reader=null;


        Writable key= (Writable) ReflectionUtils.newInstance(reader.getKeyClass(),conf);
        Writable value= (Writable) ReflectionUtils.newInstance(reader.getValueClass(),conf);

        List<SequenceFile.Writer.Option> options=new LinkedList<SequenceFile.Writer.Option>();
        SequenceFile.Writer.Option pathOpt=SequenceFile.Writer.file(new Path(destinaTion));
        SequenceFile.Writer.Option keyOpt=SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option valueOpt=SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option ComOpt=SequenceFile.Writer.compression(BLOCK);
        options.add(pathOpt);
        options.add(keyOpt);
        options.add(valueOpt);
        options.add(ComOpt);

        SequenceFile.Writer.Option[] opts= (SequenceFile.Writer.Option[]) options.toArray();

        //conf.addResource(new Path(destinaTion));

        try {
            long postion=reader.getPosition();
            while (reader.next(key,value)){
                postion=reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            IOUtils.closeStream(reader);
        }


    }



}
