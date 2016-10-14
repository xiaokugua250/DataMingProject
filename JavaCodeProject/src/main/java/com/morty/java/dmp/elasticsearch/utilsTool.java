package com.morty.java.dmp.elasticsearch;

/**
 * Created by Administrator on 2016/05/13.
 */
public class UtilsTool {
    public void mutiArgs(String... mutiArgs) {
        for (String item : mutiArgs) {
            System.out.println("args = [" + item + "]");
        }
    }

    @Override
    public String toString() {
        return super.toString();
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
