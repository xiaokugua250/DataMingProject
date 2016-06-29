package com.morty.java.dmp.hdfs;
/**
 * Created by duliang on 2016/6/19.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;

/**
 * 对MapFile再次建立索引
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 15:11
 * email:duliang1128@163.com
 */
public class MapFileFixer {

    public void init() {

    }

    /**
     * @param fileSystem
     * @param map
     * @param mapdata
     * @param configuration
     * @throws Exception
     */
    public void getMapFile(FileSystem fileSystem, Path map, Path mapdata, Configuration configuration) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, mapdata, configuration);
        Class keyclass = reader.getKeyClass();
        Class valueclass = reader.getValueClass();
        reader.close();
        Long entries = MapFile.fix(fileSystem, map, keyclass, valueclass, false, configuration);

    }


}
