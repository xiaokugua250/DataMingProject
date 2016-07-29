package com.morty.java.j2se.code.ref;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Created by IntelliJ IDEA.
 * Created by duliang on 2016/7/28.
 * Time: 10:45
 * email:duliang1128@163.com
 */


public abstract class AbsPopulator {

    public final void dataInitialing() throws Exception {
        Method[] methods = getClass().getMethods();
        for (Method method : methods) {
            if (isInitDataMethod(method)) {
                method.invoke(this);
            }
        }
        // doInit();
    }

    private boolean isInitDataMethod(Method method) {
        return method.getName().startsWith("init")
                && Modifier.isPublic(method.getModifiers())
                && method.getReturnType().equals(Void.TYPE)
                && !method.isVarArgs()
                && !Modifier.isAbstract(method.getModifiers());
    }

}
