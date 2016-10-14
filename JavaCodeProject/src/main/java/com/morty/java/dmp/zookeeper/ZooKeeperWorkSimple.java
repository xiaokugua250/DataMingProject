package com.morty.java.dmp.zookeeper;

import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;

/**
 * Created by morty on 2016/09/19.
 */
public class ZooKeeperWorkSimple implements Watcher {
    private static final Logger LOG = Logger.getLogger(ZooKeeperWorkSimple.class);
    String serverId = Integer.toHexString(new Random().nextInt());
    ZooKeeper zooKeeper;
    AsyncCallback.StringCallback createWorkerCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    register();

                    break;

                case OK:

                    // TODO: 2016/09/19 ok
                    break;

                case NODEEXISTS:

                    // TODO: 2016/09/19 nodeexisted
                    break;

                default:

                    // TODO: 2016/09/19 other opt
            }
        }
    };
    String hostPort;

    ZooKeeperWorkSimple(String hostPort) {
        this.hostPort = hostPort;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("===========watchEvent============");
    }

    void register() {
        zooKeeper.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallBack,
                null);
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 15000, this);
    }

/*
    AsyncCallback.StatCallback statusUpdateCallBack = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int i, String s, Object o, Stat stat) {
            switch (KeeperException.Code.get(i)){
                case CONNECTIONLOSS:
                    updateStatus((String) o);
                    return;

            }
        }
    };
    Synchronized  void updateStatus(String status){
        if(status == this.status){
            zooKeeper.setData("/workers/"+name,
                    status.getBytes(),
                    -1,
                    statusUpdateCallBack,status);
        }
    }

    public void setStatus(String status){
        this.status = status;
        updateStatus(status);
    }

    private void updateStatus(String o) {
    }*/
}


//~ Formatted by Jindent --- http://www.jindent.com
