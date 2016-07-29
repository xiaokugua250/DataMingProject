package com.morty.java.j2se.code.ref;

import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * Created by duliang on 2016/7/28.
 * Time: 16:03
 * email:duliang1128@163.com
 */


public class TaxCaculator implements Callable<Integer> {

    private int seedMoney;

    public TaxCaculator(int seedMoney) {
        this.seedMoney = seedMoney;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        /*ExecutorService es= Executors.newSingleThreadExecutor();
        Future<Integer> future=es.submit(new TaxCaculator(100));
        while (!future.isDone()){
            TimeUnit.MILLISECONDS.sleep(10000);
            System.out.println("#");
        }
        System.out.println("future.get() = " + future.get());
        es.shutdown();
        */
        ExecutorService es = Executors.newFixedThreadPool(2);

        for (int i = 0; i < 4; i++) {
            es.submit(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Thread.currentThread().getName() = " + Thread.currentThread().getName());
                }
            });

        }
        es.shutdown();
    }

    @Override
    public Integer call() throws Exception {
        TimeUnit.MILLISECONDS.sleep(10000);
        return seedMoney / 10;
    }

}
