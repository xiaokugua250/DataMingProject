package com.morty.java.j2se.code.ref;
/**
 * Created by duliang on 2016/7/28.
 */

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/7/28
 * Time: 10:07
 * email:duliang1128@163.com
 */
public class SubjectHandler implements InvocationHandler {
    private Subject subject;

    public SubjectHandler(Subject subject) {
        this.subject = subject;
    }

    public static void main(String[] args) {
        Subject subject = new RealSubject();
        InvocationHandler handler = new SubjectHandler(subject);
        ClassLoader cl = subject.getClass().getClassLoader();
        Subject proxy = (Subject) java.lang.reflect.Proxy.newProxyInstance(cl, subject.getClass().getInterfaces(), handler);
        proxy.request();
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("预处理");
        Object object = method.invoke(subject, args);
        System.out.println("后处理");
        return object;
    }
}
