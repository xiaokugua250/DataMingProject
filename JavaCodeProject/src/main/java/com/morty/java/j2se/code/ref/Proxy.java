package com.morty.java.j2se.code.ref;
/**
 * Created by duliang on 2016/7/28.
 */

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/7/28
 * Time: 10:03
 * email:duliang1128@163.com
 */
public class Proxy implements Subject {
    private Subject subject = null;

    public Proxy() {
        subject = new RealSubject();
    }

    public Proxy(Subject _subject) {
        this.subject = _subject;
    }


    @Override
    public void request() {

        before();
        subject.request();
        after();
    }

    private void before() {
    }

    private void after() {
    }
}
