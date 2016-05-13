package com.morty.java.j2se.code.annotations;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/05/11.
 */
public class AnnotationExample {
    public static void main(String[] args){}

    @Override
    @MethodInfo(author ="pankaj",comments = "main method",date = "Nov 17 2012",revision = 1)
    public String toString(){
        return "overriden tostring method";
    }

    @Deprecated
    @MethodInfo(comments = "deprecated method",date = "nov 17 2012")
    public static void oldMethod(){
        System.out.println("oldmethod donot use it");
    }

    @SuppressWarnings({"unchecked","deprecation"})
    @MethodInfo(author = "pankaj",comments = "main method",date = "nov 17 2012",revision = 10)
    public static void genericsTest() throws FileNotFoundException{
        List l=new ArrayList();
        l.add("abc");
        oldMethod();
    }
}
