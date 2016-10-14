package com.morty.java.dmp.zookeeper;

/**
 * Created by morty on 2016/05/27.
 */
public interface DataMonitorListener {

    /**
     * The ZooKeeper session is no longer valid.
     *
     * @param rc the ZooKeeper reason code
     */
    void closing(int rc);

    /**
     * The existence status of the node has changed.
     */
    void exists(byte data[]);
}


//~ Formatted by Jindent --- http://www.jindent.com
