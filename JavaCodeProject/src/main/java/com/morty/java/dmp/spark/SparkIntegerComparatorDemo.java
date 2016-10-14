package com.morty.java.dmp.spark;

import java.util.Comparator;

/**
 * spark comparator demo
 * usage:rdd.sortByKey(comp)
 * Created by morty on 2016/05/23.
 */
public class SparkIntegerComparatorDemo implements Comparator<Integer> {
    public int compare(Integer o1, Integer o2) {
        return String.valueOf(o1).compareTo(String.valueOf(o2));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
