package com.morty.java.dmp.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * Created by morty on 2016/09/19.
 */
public class ZooKeeperWatcherSimple implements Watcher {
    static boolean isLeader;
    String serverId = Integer.toHexString(new Random().nextInt());
    String serverIdLong = Integer.toHexString((int) new Random().nextLong());
    ZooKeeper zooKeeper;
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (org.apache.storm.shade.org.apache.zookeeper.KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    return;

                case OK:
                    isLeader = true;

                    break;

                default:
                    isLeader = false;
            }

            System.out.println("im" + (isLeader
                    ? ""
                    : "not") + "the leader");
        }
    };
    AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
            switch (org.apache.storm.shade.org.apache.zookeeper.KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    checkMaster();

                    return;

                case NONODE:
                    runForMaster();

                    return;
            }
        }
    };
    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int i, String s, Object o, String s1) {
            switch (KeeperException.Code.get(i)) {
                case CONNECTIONLOSS:
                    createParent(s, (byte[]) o);

                    break;

                case OK:
                    System.out.println("\"parent create\" = " + "parent create");

                    break;

                case NODEEXISTS:
                    System.out.println("\"parent already registered\" = " + "parent already registered");

                    break;

                default:
                    System.out.println("\"error happed\" = " + "error happed");
            }
        }
    };
    String hostPort;

    ZooKeeperWatcherSimple(String hostPort) {
        this.hostPort = hostPort;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeperWatcherSimple zooKeeperWatcherSimple = new ZooKeeperWatcherSimple(args[0]);

        zooKeeperWatcherSimple.startZk();
        Thread.sleep(60000);
    }

    public void bootstrap() {
        createParent("/worker", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte data[] = zooKeeper.getData("/master", false, stat);

                isLeader = new String(data).equals(serverId);

                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();

                return false;
            } catch (KeeperException e) {
                e.printStackTrace();
            }
        }
    }

    void checkMasterAsyc() {
        zooKeeper.getData("/master", false, masterCheckCallBack, null);
    }

    void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("watchedEvent = " + watchedEvent);
    }

    void runForMaster() {
        while (true) {
            try {
                zooKeeper.create("/master", serverId.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;

                break;
            } catch (KeeperException.NodeExistsException e) {
                isLeader = false;

                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (KeeperException e) {
                e.printStackTrace();
            }

            if (checkMaster()) {
                break;
            }
        }
    }

    void runForMasterAsyc() {
        zooKeeper.create("/master",
                serverId.getBytes(),
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null);
    }

    void runForMater() throws KeeperException, InterruptedException {
        zooKeeper.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }

    void startZk() throws IOException {
        zooKeeper = new ZooKeeper(hostPort, 1500, this);
    }

    void stopZk() throws InterruptedException {
        zooKeeper.close();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
