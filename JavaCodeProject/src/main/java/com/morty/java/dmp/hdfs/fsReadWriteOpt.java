package com.morty.java.dmp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.Progressable;
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
    InputStream          inputStream = null;
    FileSystem           fileSystem;

    /**
     * ��ʾhdfs�ļ�����
     * @param dataUri
     */
    public void fsCat(String dataUri) {
        try {
            inputStream = fileSystem.open(new Path(dataUri));
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    /**
     * �ļ�����
     * @param source
     * @param destination
     */
    public void fsCopy(String source, String destination) {
        try {
            InputStream  in           = new BufferedInputStream(new FileInputStream(source));
            OutputStream outputStream = fileSystem.create(new Path(destination));

            IOUtils.copyBytes(in, outputStream, 4096, false);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ��������д�뵽ָ��λ��
     *
     * @param inputStream
     * @param des
     * @param params
     * @throws IOException
     */
    public void fsLoadData(InputStream inputStream, String des, String... params) throws IOException {
        FileSystem   fileSystem;
        OutputStream outputStream = null;

        try {
            fileSystem   = FileSystem.get(URI.create(des), conf);
            outputStream = fileSystem.create(new Path(des),
                                             new Progressable() {
                                                 @Override
                                                 public void progress() {
                                                     System.out.println(".");
                                                 }
                                             });
        } catch (IOException e) {
            e.printStackTrace();
        }

        IOUtils.copyBytes(inputStream, outputStream, 4096, true);
    }

    /**
     * д���ݵ�fs
     *
     * @param fileSystem
     * @param path
     * @param bytes
     * @param params
     */
    public void fsWtrite(FileSystem fileSystem, Path path, byte[] bytes, String... params) {
        FSDataOutputStream fsoutputStream;

        try {
            fsoutputStream = fileSystem.create(path);
            fsoutputStream.write(bytes);
            fsoutputStream.flush();
            fsoutputStream.hflush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * ��ȡ�ļ�����Ϣ
     * @param source
     * @return
     */
    public Object getfSAndBlockStats(String source) {
        try {
            FileStatus fileStatus = fileSystem.getFileStatus(new Path(source));

            // �ļ�Ԫ����
            fileStatus.getAccessTime();
            fileStatus.getBlockSize();
            fileStatus.getModificationTime();

            // ...�ȵ�

            BlockLocation[] blockLocations = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

            /*
             * for(BlockLocation blockLocation: blockLocations){
             *   String[] cachedHosts= blockLocation.getCachedHosts();
             *   String[] hosts=blockLocation.getHosts();
             *   String[] names=blockLocation.getNames();
             *   long lens=blockLocation.getLength();
             * }
             */
            return blockLocations;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void init() {
        conf = new Configuration();

        try {
            fileSystem = FileSystem.get(URI.create(DATA_SOURCE_URI), conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * fsstatus ��ӹ���
     *
     * @param fs
     * @param fsPath
     * @param regex
     * @param params
     * @return
     */
    public FileStatus[] pathFilterOpt(FileSystem fs, String fsPath, String regex, String... params) {
        FileStatus[] fileStatus;

        try {
            fileStatus = fs.globStatus(new Path(fsPath),
                                       new PathFilter() {
                                           @Override
                                           public boolean accept(Path path) {
                                               return !path.toString().matches(regex);
                                           }
                                       });

            return fileStatus;
        } catch (IOException e) {
            e.printStackTrace();

            return null;
        }
    }

    /**
     *
     * @param text
     * @param destinaTion
     */
    public void readSeqFile(Object[] text, String destinaTion) {
        SequenceFile.Reader              reader   = null;
        Writable                         key      = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable                         value    = (Writable) ReflectionUtils.newInstance(reader.getValueClass(),
                                                                                           conf);
        List<SequenceFile.Writer.Option> options  = new LinkedList<SequenceFile.Writer.Option>();
        SequenceFile.Writer.Option       pathOpt  = SequenceFile.Writer.file(new Path(destinaTion));
        SequenceFile.Writer.Option       keyOpt   = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option       valueOpt = SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option       ComOpt   = SequenceFile.Writer.compression(BLOCK);

        options.add(pathOpt);
        options.add(keyOpt);
        options.add(valueOpt);
        options.add(ComOpt);

        SequenceFile.Writer.Option[] opts = (SequenceFile.Writer.Option[]) options.toArray();

        // conf.addResource(new Path(destinaTion));
        try {
            long postion = reader.getPosition();

            while (reader.next(key, value)) {
                postion = reader.getPosition();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    /**
     *
     * @param text
     * @param destinaTion
     */
    public void writSeqFile(Object[] text, String destinaTion) {
        SequenceFile.Writer              writer   = null;
        IntWritable                      key      = new IntWritable();
        Text                             value    = new Text();
        List<SequenceFile.Writer.Option> options  = new LinkedList<SequenceFile.Writer.Option>();
        SequenceFile.Writer.Option       pathOpt  = SequenceFile.Writer.file(new Path(destinaTion));
        SequenceFile.Writer.Option       keyOpt   = SequenceFile.Writer.keyClass(key.getClass());
        SequenceFile.Writer.Option       valueOpt = SequenceFile.Writer.valueClass(value.getClass());
        SequenceFile.Writer.Option       ComOpt   = SequenceFile.Writer.compression(BLOCK);

        options.add(pathOpt);
        options.add(keyOpt);
        options.add(valueOpt);
        options.add(ComOpt);

        SequenceFile.Writer.Option[] opts = (SequenceFile.Writer.Option[]) options.toArray();

        // conf.addResource(new Path(destinaTion));
        try {
            writer = SequenceFile.createWriter(conf, opts);

            // todo  д���������߼�
            for (int i = 0; i <= 100; i++) {
                key.set(100 - i);
                value.set((byte[]) text[i % text.length]);
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    /**
     * ��ȡfs
     * @param dataUri
     * @return
     */
    public FileSystem getFileSystem(String dataUri) {
        try {
            fileSystem = FileSystem.get(URI.create(dataUri), conf);

            return fileSystem;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * ����fs��Ϣ
     *
     * @param fileSystem
     * @param path
     * @param params
     * @return
     */
    public FileStatus[] getFsStatus(FileSystem fileSystem, Path path, String... params) {
        FileStatus[] fileStatus;

        try {
            fileStatus = fileSystem.listStatus(path);

            return fileStatus;
        } catch (IOException e) {
            e.printStackTrace();

            return null;
        }
    }

    /**
     * ��ȡfsdatainputStream
     *
     * @param fileSystem
     * @param path
     * @param params
     * @return
     */
    public FSDataInputStream getFsStream(FileSystem fileSystem, Path path, String... params) {
        FSDataInputStream fsDataInputStream;

        try {
            fsDataInputStream = fileSystem.open(path);

            // fsDataInputStream.seek(station); �����ļ�λ��
            return fsDataInputStream;
        } catch (IOException e) {
            e.printStackTrace();

            return null;
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
