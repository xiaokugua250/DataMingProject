package com.morty.java.dmp.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * java spark 启动和监控类
 * Created by morty on 2016/05/25.
 */
public class SparkLauncherAndMonitor {

    public SparkConf sparkconf;
    public JavaSparkContext javaSparkContext;
    public Configuration conf;
    public SparkLauncher sparkLauncher;
    public SparkAppHandle sparkAppHandle;
    Logger LOG = Logger.getLogger(SparkLauncherAndMonitor.class.getName());

    /**
     * 初始化方法，可根据properties文件或xml文件进行设置
     */
    // TODO: 2016/05/25  初始化
    public void init() {
        //// TODO: 2016/05/25  设置sparkconf
        sparkconf = new SparkConf().setAppName("simpleApp");

        //// TODO: 2016/05/25  设置sparkcontext
        javaSparkContext = new JavaSparkContext(sparkconf);
        conf = new Configuration();

    }


    public void setSparkLaunchByHandle() throws IOException {

        sparkAppHandle = new SparkLauncher()
                .setAppResource("dir/spark/sparkApp.jar")
                .setMainClass("spark.app.Main")
                .setMaster("cluster")
                .setConf(SparkLauncher.DRIVER_MEMORY, "8g")
                .startApplication(new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle sparkAppHandle) {
                        // TODO: 2016/05/25  状态改变时的操作
                        SparkAppHandle.State sparkState = sparkAppHandle.getState();
                        SparkAppHandle.State[] states = sparkState.values();
                        for (SparkAppHandle.State state : states) {
                            LOG.info(state.name());
                        }
                    }

                    @Override
                    public void infoChanged(SparkAppHandle sparkAppHandle) {
                        // TODO: 2016/05/25  状态改变时的操作
                        LOG.info(sparkAppHandle.getAppId());

                        LOG.info(sparkAppHandle.getState());
                    }
                });

    }

    public void sparkLaunchByLaunch() throws IOException, InterruptedException {
        Process spark = sparkLauncher.setAppName("appName")
                .setAppResource("dir/spark/app/jar")
                .setMainClass("spark.app.Main")
                .setConf(SparkLauncher.DRIVER_MEMORY, "8g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "16")
                .setDeployMode("cluster")
                .setMaster("master")
                .launch();
        spark.waitFor();

    }


}
