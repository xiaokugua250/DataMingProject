package com.morty.java.j2se.code.ref;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by IntelliJ IDEA.
 * Created by duliang on 2016/7/28.
 * Time: 10:25
 * email:duliang1128@163.com
 */


public class Decorate implements Animal {
    private Animal animal;
    private Class<? extends Feature> clz;

    public Decorate(Animal animal, Class<? extends Feature> _clz) {
        this.animal = animal;
        clz = _clz;
    }

    @Override
    public void doStuff() {
        InvocationHandler handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                Object obj = null;
                if (Modifier.isPublic(method.getModifiers())) {
                    obj = method.invoke(clz.newInstance(), args);
                }
                animal.doStuff();
                return obj;
            }
        };
        ClassLoader cl = getClass().getClassLoader();
        Feature proxy = (Feature) java.lang.reflect.Proxy.newProxyInstance(cl, clz.getInterfaces(), handler);
        proxy.load();
    }
}
