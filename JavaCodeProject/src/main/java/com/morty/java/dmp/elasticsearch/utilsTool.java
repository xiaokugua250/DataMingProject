package com.morty.java.dmp.elasticsearch;

/**
 * Created by Administrator on 2016/05/13.
 */
public class utilsTool {
    @Override
    public String toString() {
        return super.toString();
    }

    public void mutiArgs(String ... mutiArgs){

        for(String item:mutiArgs){
        System.out.println("args = [" + item+ "]");
        }
    }
}
