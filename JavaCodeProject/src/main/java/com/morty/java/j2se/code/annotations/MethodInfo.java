package com.morty.java.j2se.code.annotations;

import java.lang.annotation.*;

/**
 * Created by Administrator on 2016/05/11.
 */
@Documented
@Target(ElementType.METHOD)
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface MethodInfo {
    String author() default "pankaj";

    String date();

    int revision() default 1;
    String comments();
}


//~ Formatted by Jindent --- http://www.jindent.com
