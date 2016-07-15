package com.morty.java.dmp.dmpConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * 通用配置类
 * Created by morty on 2016/07/15.
 */
public class DmpConfigurtion extends Configuration {

    private static final Logger LOG = Logger.getLogger(DmpConfigurtion.class);
    private static final int LOAD_ZKCONF_TRY_MAX_COUNT = 5;
    private static final String[] confParams = {
            // 暂定不从ZK中获取，所以注释
            // DmpConsts.STR_ES_CLUSTER_NAME, DmpConsts.STR_ES_HOSTS, DmpConsts.STR_HIVE_DMP_URL,
            // DmpConsts.STR_HIVE_DMP_DATA_LOCATION
    };
    private static final String[] servParams = {};
    private static final String CONFIG_ALL_LOAD = "hanuman.configuration.config_all_load";
    private static DmpConfigurtion conf = null;

    /**
     * 非单例获取DmpConfiguration,如非特别情况需要，建议使用单实例
     *
     * @param copyConf
     */
    public DmpConfigurtion(Configuration copyConf) {
        init(copyConf);
    }

    public DmpConfigurtion() {
        init(null);
    }

    public static DmpConfigurtion getInstance() {
        if (conf == null) {
            synchronized (DmpConfigurtion.class) {
                if (conf == null)
                    conf = new DmpConfigurtion();
            }
        }
        return conf;
    }

    /**
     * 单实例获取DmpConfiguration，同时把copyConf的参数全部复制进新的conf对象
     *
     * @param copyConf
     * @return
     */
    public static DmpConfigurtion getInstance(Configuration copyConf) {
        if (conf == null) {
            synchronized (DmpConfigurtion.class) {
                if (conf == null)
                    conf = new DmpConfigurtion(copyConf);
            }
        }
        return conf;
    }

    public static void main(String[] args) {
        DmpConfigurtion configurtion = DmpConfigurtion.getInstance();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(configurtion);
            fs.create(new Path("duliang_test"), true);
            FileStatus fileStatus = fs.getFileStatus(new Path("/"));

            System.out.println("fileStatus = " + fileStatus
                    + "fileStatus.getOwner" + fileStatus.getOwner() +
                    "fileStatus.getBlockSize" + fileStatus.getBlockSize() +
                    "fileStatus.getPath" + fileStatus.getPath()
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean init(Configuration copyConf) {
        this.addResource("dmpConfiguration/hdfs-default.xml");
        this.addResource("dmpConfiguration/core-default.xml");
        this.addResource("dmpConfiguration/mapred-default.xml");
        this.addResource("dmpConfiguration/hdfs-default.xml");
        LOG.info("Get Configuration ...");
        LOG.info("hbase.zookeeper.quorum: " + this.get("hbase.zookeeper.quorum"));
        int count = 0;
        while (true) {
            try {
                /*
                * 加载ZK 配置
                * */
                // ConfigUtil.fastLoadConfig(this, DmpConsts.ORG, DmpConsts.APP, confParams, servParams);
                this.setBoolean(CONFIG_ALL_LOAD, true);
                break;
            } catch (Exception e) {
                LOG.error(e);
                LOG.warn("准备重新load参数，总共重试5次。");
            }
            count++;
            if (count >= LOAD_ZKCONF_TRY_MAX_COUNT) {
                return false;
            }
        }

        //copy配置
        if (copyConf != null) {
            for (Map.Entry<String, String> entry : copyConf) {
                this.set(entry.getKey(), entry.getValue());
                //  System.out.println("entry = " + entry.getKey());
            }
        }

        // 解决因为maven-assembly导致的配置冲突的问题，否则可能报错“No FileSystem for scheme: hdfs”
        // The FileSystem for hdfs: uris.
        this.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        // The FileSystem for local: uris.
        this.set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem");

        LOG.warn("Get midea-dmp Configuration finshed.");
        return true;
    }
}

