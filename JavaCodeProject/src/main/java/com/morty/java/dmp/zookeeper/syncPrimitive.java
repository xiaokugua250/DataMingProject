package com.morty.java.dmp.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Random;

/**
 * Created by duliang on 2016/5/22.
 */
public class SyncPrimitive implements Watcher {

    static ZooKeeper zk=null;
    static Integer mutex;
    String root;

    SyncPrimitive(String address){
        if(zk == null){
            try{
                System.out.println("Starting zk:");
                zk=new ZooKeeper(address,3000,this);
                mutex=new Integer(-1);
                System.out.println("finished starting zk = " + zk);
            }catch (IOException e){
                e.printStackTrace();
                zk=null;
            }
        }   //else { mutex = new Integer(-1);}
    }

    public static void main(String args[]) {
        if (args[0].equals("qTest"))
            queueTest(args);
        else
            barrierTest(args);
    }

    public static void queueTest(String args[]) {
        Queue q = new Queue(args[1], "/app1");
        System.out.println("Input: " + args[1]);
        int i;
        Integer max = new Integer(args[2]);
        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try{
                    q.produce(10 + i);
                } catch (KeeperException e){
                } catch (InterruptedException e){
                }
        } else {
            System.out.println("Consumer");
            for (i = 0; i < max; i++) {
                try{
                    int r = q.consume();
                    System.out.println("Item: " + r);
                } catch (KeeperException e){
                    i--;
                } catch (InterruptedException e){
                }
            }
        }
    }

    public static void barrierTest(String args[]) {
        Barrier b = new Barrier(args[1], "/b1", new Integer(args[2]));
        try{
            boolean flag = b.enter();
            System.out.println("Entered barrier: " + args[2]);
            if(!flag) System.out.println("Error when entering the barrier");
        } catch (KeeperException e){
        } catch (InterruptedException e){
        }
        // Generate random integer
        Random rand = new Random();
        int r = rand.nextInt(100);
        // Loop for rand iterations
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }
        try{
            b.leave();
        } catch (KeeperException e){
        } catch (InterruptedException e){
        }
        System.out.println("Left barrier");
    }

    @Override
     synchronized  public void process(WatchedEvent watchedEvent) {
        synchronized (mutex){
            System.out.println("watchedEvent.getType() = " + watchedEvent.getType());
            mutex.notify();
        }
    }

    static  public class Barrier extends SyncPrimitive{
        int size;
        String name;

        public Barrier(String address, String root,int size) {
            super(address);
            this.root=root;
            this.size = size;

            if(zk != null){
                try{
                    Stat s=zk.exists(root,false);
                    if(s == null){
                        zk.create(root,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    }
                }catch (KeeperException e){
                    System.out
                            .println("Keeper exception when instantiating queue: "
                                    + e.toString());
                    e.printStackTrace();

                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
            try{
                name=new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
            }catch (UnknownHostException e){
                e.printStackTrace();
            }
        }

        boolean enter() throws KeeperException,InterruptedException{
            zk.create(root+"/"+name,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
            while (true){
                synchronized (mutex){
                    List<String > list=zk.getChildren(root,true);
                    if(list.size()<size){
                        mutex.wait();
                    }else {
                        return true;
                    }
                }
            }
        }

        boolean leave() throws KeeperException,InterruptedException{
            zk.delete(root+"/"+name,0);
            while (true){
                synchronized (mutex){
                    List<String> list=zk.getChildren(root,true);
                    if(list.size() > 0){
                        mutex.wait();
                    }else {
                        return true;
                    }
                }
            }
        }
    }

    static public class Queue extends SyncPrimitive{


        public Queue(String address,String name) {
            super(address);
            this.root=name;
            if(zk != null){
                try {
                    Stat s=zk.exists(root,false);
                    if(s == null){
                        zk.create(root,new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
                    }
                }catch (KeeperException e){
                    e.printStackTrace();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        }

        /**
         * Add element to the queue.
         * @param i
         * @return
         * @throws KeeperException
         * @throws InterruptedException
         */
        boolean produce(int i)throws  KeeperException,InterruptedException{

            java.nio.ByteBuffer b= java.nio.ByteBuffer.allocate(4);

            byte[] value;
            b.putInt(i);
            value=b.array();
            zk.create(root+"/element",value, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
            return true;

        }

        int consume() throws KeeperException,InterruptedException{

            int retValue=-1;
            Stat stat=null;

            while (true){
                synchronized (mutex){
                    List<String> list=zk.getChildren(root,true);
                    if(list.size() == 0){
                        System.out.println("goint to wait....");
                        mutex.wait();
                    }else {
                        Integer min=new Integer(list.get(0).substring(7));
                        for(String s : list){
                            Integer tempValue=new Integer(s.substring(7));
                            System.out.println("tempValue = " + tempValue);
                            if(tempValue < min) min = tempValue;
                        }
                        System.out.println("temp value = " +root+"/element"+min);

                        byte[] b=zk.getData(root+"/element"+min,false,stat);
                        zk.delete(root+"/element"+min,0);
                        java.nio.ByteBuffer buffer= java.nio.ByteBuffer.wrap(b);
                        retValue=buffer.getInt();
                        return  retValue;

                    }
                }
            }
        }


    }

}