package com.morty.java.j2se.code.annotations;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 * Created by Administrator on 2016/05/11.
 */
public class AnnotationParsing {
    public static void main(String[] args) {
        try {
            for (Method method : AnnotationParsing.class.getClassLoader()
                    .loadClass(
                            "com.morty.java.j2se.code.annotations.AnnotationExample")
                    .getMethods()) {
                if (method.isAnnotationPresent(com.morty.java.j2se.code.annotations.MethodInfo.class)) {
                    try {
                        for (Annotation anno : method.getDeclaredAnnotations()) {
                            System.out.println("anno = " + anno);
                        }

                        MethodInfo methodAnno = method.getAnnotation(MethodInfo.class);

                        if (methodAnno.revision() == 1) {
                            System.out.println("Method with revision no 1 " + method);
                        }
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
