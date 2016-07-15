package com.morty.java.dmp.dmpConfig;

/**
 * Created by morty on 2016/07/15.
 */

/*

import com.yeezhao.commons.config.Namespace;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ILineParser;
import com.yeezhao.commons.zookeeper.ZkUtil;
import com.yeezhao.commons.zookeeper.ZooKeeperFacade;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

public class ParamConfiguration implements ILineParser {
    private static Log LOG = LogFactory.getLog(ParamConfiguration.class);
    private ZooKeeperFacade zkf;
    private String zkPathPrefix;

    public ParamConfiguration(Configuration zkConf, String orgName, String appName) {
        this.zkf = ZkUtil.getZkINstance(zkConf);
        this.zkPathPrefix = Namespace.getParamConfPath(orgName, appName);
    }

    public ParamConfiguration(ZooKeeperFacade zkf, String orgName, String appName) {
        this.zkf = zkf;
        this.zkPathPrefix = Namespace.getParamConfPath(orgName, appName);
    }

    public String getConfigPath() {
        return this.zkPathPrefix;
    }

    public void setParam(String name, String value) {
        ZkUtil.checkAndCreateZnode(this.zkf, this.zkPathPrefix);
        String zkPath = this.zkPathPrefix + '/' + name;
        if(this.zkf.znodeExist(zkPath, (Watcher)null)) {
            this.zkf.setZNodeData(zkPath, Bytes.toBytes(value));
        } else {
            this.zkf.createZNode(zkPath, Bytes.toBytes(value), CreateMode.PERSISTENT);
        }

    }

    public String getParam(String name) throws KeeperException, InterruptedException {
        String zkPath = this.zkPathPrefix + '/' + name;
        return this.zkf.znodeExist(zkPath, (Watcher)null)?Bytes.toString(this.zkf.znodeData(zkPath, (Watcher)null)):null;
    }

    public void loadParams(String confFile) throws FileNotFoundException, IOException {
        AdvFile.loadFileInDelimitLine(new FileInputStream(confFile), this);
    }

    public void delAll() {
        System.out.println("Delete all params under " + this.zkPathPrefix);
        this.zkf.znodeDeleteRecursive(this.zkPathPrefix);
    }

    public void delParam(String param) {
        System.out.println("Delete param: " + this.zkPathPrefix + '/' + param);
        this.zkf.znodeDelete(this.zkPathPrefix + '/' + param);
    }

    public void printAll() throws KeeperException, InterruptedException {
        if(!this.zkf.znodeExist(this.zkPathPrefix)) {
            System.out.println("No parameters configured.");
        } else {
            List childNodeList = this.zkf.znodeChildren(this.zkPathPrefix);
            if(childNodeList != null && !childNodeList.isEmpty()) {
                Iterator i$ = childNodeList.iterator();

                while(i$.hasNext()) {
                    String c = (String)i$.next();
                    System.out.println(String.format("\t%s=%s", new Object[]{c, Bytes.toString(this.zkf.znodeData(this.zkPathPrefix + '/' + c))}));
                }

            } else {
                System.out.println("No parameters configured.");
            }
        }
    }

    public void printParam(String param) throws KeeperException, InterruptedException {
        if(!this.zkf.znodeExist(this.zkPathPrefix + '/' + param)) {
            System.out.println("No such parameter configured.");
        } else {
            System.out.println(String.format("\t%s=%s", new Object[]{param, Bytes.toString(this.zkf.znodeData(this.zkPathPrefix + '/' + param))}));
        }
    }

    public void parseLine(String line) {
        int pos = line.indexOf(61);
        if(pos != -1 && pos != 0 && pos != line.length() - 1) {
            String name = line.substring(0, pos);
            String value = line.substring(pos + 1);
            this.setParam(name, value);
            LOG.info(String.format("configuring...%s=%s", new Object[]{name, value}));
        } else {
            LOG.error("Invalid config line ignored (fail to parse name=value): " + line);
        }
    }
}
*/