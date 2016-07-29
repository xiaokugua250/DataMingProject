package com.morty.java.j2se.code.ref;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * Created by IntelliJ IDEA.
 * Created by duliang on 2016/7/28.
 * Time: 16:43
 * email:duliang1128@163.com
 */


public class Runner implements Callable<Integer> {

    private CountDownLatch begin;
    private CountDownLatch end;

    public Runner(CountDownLatch begin, CountDownLatch end) {
        this.begin = begin;
        this.end = end;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        int num = 10;
        CountDownLatch begin = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(num);
        ExecutorService es = Executors.newFixedThreadPool(num);
        List<Future<Integer>> futures = new ArrayList<Future<Integer>>();
        for (int i = 0; i < num; i++) {
            futures.add(es.submit(new Runner(begin, end)));
        }
        begin.countDown();
        end.await();
        int count = 0;
        for (Future<Integer> f : futures) {
            count += f.get();
        }
        System.out.println("count = " + count / num);
    }

    @Override
    public Integer call() throws Exception {
        int score = new Random().nextInt(25);
        begin.await();
        TimeUnit.MILLISECONDS.sleep(score);
        end.countDown();
        return score;
    }
}
