package com.morty.java.j2se.code.sortUtils;

import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

/**
 * Created by duliang on 2016/5/15.
 *
 * collator类可用于简单排序
 */
public class sortTools {



    /**
     *
     * @param str
     */
    public  static void  sordStrBysimChinese(String[] str){
        Comparator cmp= Collator.getInstance(Locale.SIMPLIFIED_CHINESE);  //简体中文排序
        Arrays.sort(str,cmp);

    }

}
