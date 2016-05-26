package com.morty.java.dmp.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

/**
 * Created by morty on 2016/05/26.
 */
public class ZkExcutor implements Watcher, Runnable, DataMonitor.DataMonitor.listener {

    String znode;
    ZkMonitor monitor;
    ZooKeeper zooKeeper;
    String filename;
    String exec[];
    Process child;

    public ZkExcutor(String hostport, String znode, String filename, String exec[]) throws KeeperException, IOException {
        this.filename = filename;
        this.exec = exec;
        zooKeeper = new ZooKeeper(hostport, 3000, this);
        monitor = new ZkMonitor();
    }

    @Override
    public void process(WatchedEvent event) {

    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                while ()
            }
        }

    }
}
