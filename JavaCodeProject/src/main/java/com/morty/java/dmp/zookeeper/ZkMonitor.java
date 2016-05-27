package com.morty.java.dmp.zookeeper;


import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;

/**
 * Created by morty on 2016/05/26.
 */
public class ZkMonitor implements Watcher, AsyncCallback.StatCallback {
    ZooKeeper zk;

    String znode;

    Watcher chainedWatcher;

    boolean dead;

    DataMonitorListener listener;

    byte prevData[];

    public ZkMonitor(ZooKeeper zk, String znode, Watcher chainWatcher, DataMonitorListener dataMonitorListener) {
        this.zk = zk;
        this.znode = znode;
        this.chainedWatcher = chainWatcher;
        this.listener = dataMonitorListener;
        // Get things started by checking if the node exists. We are going
        // to be completely event driven
        zk.exists(znode, true, this, null);
    }


    public void processResult(int rc, String path, Object ctx, Stat stat) {
        boolean exits;
        switch (rc) {
            case Code.OK:
                exits = true;
                break;
            ;
            case Code.NONODE:
                exits = false;
                break;
            case Code.SESSIONEXPIRED:
                break;
            case Code.AUTHFAILED:
                listener.closing(rc);
                return;
            default:
                zk.exists(znode, true, this, null);
                return;

        }
        byte[] b = null;
        if (exits) {
            try {
                b = zk.getData(znode, false, null);

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }
        }
        if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
            listener.exists(b);
            prevData = b;
        }
    }

    @Override
    public void process(WatchedEvent event) {
        String path = event.getPath();
        // We are are being told that the state of the
        // connection has changed
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    // In this particular example we don't need to do anything
                    // here - watches are automatically re-registered with
                    // server and any watches triggered while the client was
                    // disconnected will be delivered (in order of course)
                    break;
                case Expired:
                    dead = true;
                    listener.closing(Code.SESSIONEXPIRED);
                    break;

            }
        } else {
            if (path != null && path.equals(znode)) {
                zk.exists(znode, true, this, null);
            }
        }
        if (chainedWatcher != null) {
            chainedWatcher.process(event);
        }
    }
}
