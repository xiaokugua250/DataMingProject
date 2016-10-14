package com.morty.java.dmp.dmpConfig;

/*import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

*/
/**
 * Created by morty on 2016/07/15.
 */
/*
public class ConfigUtil {
 protected static Log LOG = LogFactory.getLog(ConfigUtil.class);

 public ConfigUtil() {
 }

 public static void registerSimpleServ(Configuration zkConf, String org, String app, String servName, String servUrl) throws KeeperException.NodeExistsException {
     ServConfiguration servConfig = new ServConfiguration(zkConf, org, app, servName);
     servConfig.setProperty("serv.url", servUrl);
     servConfig.registerServ();
 }

 public static void registerSimpleServ(Configuration zkConf, String org, String app, String servName, String servType, String servPort) throws KeeperException.NodeExistsException {
     registerSimpleServ(zkConf, org, app, servName, genServiceUrl(servType, servPort, servName));
 }

 private static String genServiceUrl(String servType, String servPort, String servName) {
     String hostName = null;

     try {
         hostName = InetAddress.getLocalHost().getHostName();
     } catch (UnknownHostException var5) {
         LOG.error("Failed to get local hostname.", var5);
         hostName = "_UNK";
     }

     return String.format("%s://%s:%s/%s", new Object[]{servType, hostName, servPort, servName});
 }

 public static String getServiceUrl(Configuration zkConf, String org, String app, String servName) throws KeeperException, InterruptedException {
     ServConfiguration servConfig = ServConfiguration.loadServConfiguration(zkConf, org, app, servName);
     return servConfig.getProperty("serv.url");
 }

 public static void fastLoadConfig(Configuration zkConf, String org, String app, String[] paramNamesToLoad, String[] simpleServNamesToLoad) throws KeeperException, InterruptedException, ConfigNotFoundException {
     int i$;
     String value;
     if(paramNamesToLoad != null && paramNamesToLoad.length > 0) {
         ParamConfiguration arr$ = new ParamConfiguration(zkConf, org, app);
         System.out.println("loading params " + paramNamesToLoad.length);
         String[] len$ = paramNamesToLoad;
         i$ = paramNamesToLoad.length;

         for(int servName = 0; servName < i$; ++servName) {
             value = len$[servName];
             String value1 = arr$.getParam(value);
             System.out.println("  " + value + ": " + value1);
             if(value1 == null) {
                 throw new ConfigNotFoundException("can not load param config: " + value);
             }

             zkConf.set(value, value1);
         }
     } else {
         System.out.println("no params to load");
     }

     if(simpleServNamesToLoad != null && simpleServNamesToLoad.length > 0) {
         System.out.println("loading simpleServs " + simpleServNamesToLoad.length);
         String[] var11 = simpleServNamesToLoad;
         int var12 = simpleServNamesToLoad.length;

         for(i$ = 0; i$ < var12; ++i$) {
             String var13 = var11[i$];
             value = getServiceUrl(zkConf, org, app, var13);
             System.out.println("  " + var13 + ": " + value);
             if(value == null) {
                 throw new ConfigNotFoundException("can not load service config: " + var13 + ":" + "serv.url");
             }

             zkConf.set(var13, value);
         }
     } else {
         System.out.println("no servs to load");
     }

 }


}*/


//~ Formatted by Jindent --- http://www.jindent.com
