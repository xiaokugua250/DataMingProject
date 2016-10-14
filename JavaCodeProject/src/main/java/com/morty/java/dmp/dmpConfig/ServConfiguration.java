package com.morty.java.dmp.dmpConfig;

/**
 * Created by morty on 2016/07/15.
 */
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

/*
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.yeezhao.commons.config.Namespace;
import com.yeezhao.commons.zookeeper.MonitorHandler;
import com.yeezhao.commons.zookeeper.ZkUtil;
import com.yeezhao.commons.zookeeper.ZooKeeperFacade;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.NodeExistsException;

public class ServConfiguration {
    protected static Log LOG = LogFactory.getLog(ServConfiguration.class);
    Configuration zkConf = null;
    private String zkServConfPath;
    private Map<String, String> servProps = new HashMap();

    public static ServConfiguration loadServConfiguration(Configuration zkConf, String org, String app, String servName) throws KeeperException, InterruptedException {
        ServConfiguration servConf = new ServConfiguration(zkConf, org, app, servName);
        servConf.loadServ();
        return servConf;
    }

    public static ServConfiguration createServConfiguration(Configuration zkConf, String org, String app, String servName) {
        ServConfiguration servConf = new ServConfiguration(zkConf, org, app, servName);
        return servConf;
    }

    public ServConfiguration(Configuration zkConf, String org, String app, String servName) {
        this.zkConf = zkConf;
        this.zkServConfPath = Namespace.getServConfPath(org, app, servName);
    }
/*
    public ZooKeeperFacade registerServ() throws KeeperException.NodeExistsException {
        this.setProperty("startTime", Long.toString(System.currentTimeMillis()));
        return ZkUtil.createNotExpireZkFacade(this.zkConf, getMonitorHandler(this));
    }

    public boolean loadServ() throws KeeperException, InterruptedException {
        ZooKeeperFacade zkf = ZkUtil.createNotExpireZkFacade(this.zkConf);
        if(!zkf.znodeExist(this.zkServConfPath)) {
            return false;
        } else {
            List childNodes = zkf.znodeChildren(this.zkServConfPath);
            if(childNodes != null && !childNodes.isEmpty()) {
                Gson gson = new Gson();
                Random rand = new Random(System.currentTimeMillis());
                String node = (String)childNodes.get(rand.nextInt(childNodes.size()));
                String servPropsJson = Bytes.toString(zkf.znodeData(this.zkServConfPath + '/' + node));
                this.servProps = (Map)gson.fromJson(servPropsJson, (new TypeToken() {
                }).getType());
                return true;
            } else {
                LOG.error("No host offer the service requested: " + this.zkServConfPath);
                return false;
            }
        }
    }

    public void setProperty(String prop, String value) {
        this.servProps.put(prop, value);
    }

    public String getProperty(String prop) {
        return (String)this.servProps.get(prop);
    }

    public boolean containsProperty(String prop) {
        return this.servProps.containsKey(prop);
    }

    private String getZkServConfPath() {
        return this.zkServConfPath;
    }

    private String getServPropsInJson() {
        Gson gson = new Gson();
        return gson.toJson(this.servProps);
    }

    private static MonitorHandler getMonitorHandler(final ServConfiguration servConf) throws KeeperException.NodeExistsException {
        return new MonitorHandler() {
            public void execute(ZooKeeperFacade zkf) throws KeeperException.NodeExistsException {
                String zkPath = servConf.getZkServConfPath();
                ZkUtil.checkAndCreateZnode(zkf, zkPath);
                String hostName = null;

                try {
                    hostName = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException var5) {
                    ServConfiguration.LOG.error("Failed to register server with local hostname.", var5);
                    hostName = "_UNK";
                }

                if(zkf.znodeExist(zkPath + '/' + hostName, (Watcher)null)) {
                    throw new KeeperException.NodeExistsException(zkPath + '/' + hostName);
                } else {
                    zkf.createZNode(zkPath + '/' + hostName, Bytes.toBytes(servConf.getServPropsInJson()), CreateMode.EPHEMERAL);
                    ServConfiguration.LOG.info(String.format("Register service: %s (%s)", new Object[]{zkPath, hostName}));
                }
            }
        };
    }
}
*/


//~ Formatted by Jindent --- http://www.jindent.com
