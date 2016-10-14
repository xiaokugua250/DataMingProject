package com.morty.java.dmp.spark;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created by morty on 2016/05/23.
 */
public class SparkInfo {
    public static final List<String> ApacheLogs =
            Lists.newArrayList(
                    "10.10.10.10 - \"FRED\" [18/Jan/2013:17:56:07 +1100] \"GET http://images.com/2013/Generic.jpg "
                            + "HTTP/1.1\" 304 315 \"http://referall.com/\" \"Mozilla/4.0 (compatible; MSIE 7.0; "
                            + "Windows NT 5.1; GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; "
                            + ".NET CLR 3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR "
                            + "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.350 \"-\" - \"\" 265 923 934 \"\" "
                            + "62.24.11.25 images.com 1358492167 - Whatup",
                    "10.10.10.10 - \"FRED\" [18/Jan/2013:18:02:37 +1100] \"GET http://images.com/2013/Generic.jpg "
                            + "HTTP/1.1\" 304 306 \"http:/referall.com\" \"Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; "
                            + "GTB7.4; .NET CLR 2.0.50727; .NET CLR 3.0.04506.30; .NET CLR 3.0.04506.648; .NET CLR "
                            + "3.5.21022; .NET CLR 3.0.4506.2152; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR "
                            + "3.5.30729; Release=ARP)\" \"UD-1\" - \"image/jpeg\" \"whatever\" 0.352 \"-\" - \"\" 256 977 988 \"\" "
                            + "0 73.23.2.15 images.com 1358492557 - Whatup");
    public static final int TF = 1;
    public static final int IDF = 2;
    public static final int WORD2VEC = 3;
    public static final int COUNTVECTORIZER = 4;
    public static final int TOKENZER = 5;
    public static final int STOPWORDREMOVER = 6;
    public static final int NGRAM = 7;

    // TODO �μ�
    // http://spark.apache.org/docs/latest/ml-features.html
    public static final int BINARIZER = 8;
    public static final int PCA = 9;
    public static final int POLYNOMIALEXPRASSION = 10;
    public static final int DCT = 11;
    public static final int STRINGINDEXER = 12;
    public static final int INDEXTOSTRING = 13;
    public static final int ONEHOTENCODEER = 14;
    public static final int VECTORINDEXER = 15;
    public static final int NORMAILIZER = 16;
    public static final int STANDARDSCALER = 17;
    public static final int MINMAXSCALER = 18;
    public static final int BUCKERTIZER = 19;
    public static final int ELEMNETTWISEPRODUCT = 20;
    public static final int SQLTRANSFORMER = 21;
    public static final int VECTORASSERMBLER = 22;
    public static final int QUANLITIEDISCRETIZER = 23;
    public static final int VECTORSLICER = 25;
    public static final int RFORMAULA = 26;
    public static final int CHISQSELECTOR = 27;
    public static String HDFS_FILE = "hdfs://dev:port/hdfs/data";
    public static String HBASE_ROOTDIR = "hdfs://dev:port/hbase";
    public static String ZKQUORUM = "zkHost";
    public static String HBASEOUTTABLE = "hbase_out_table";
    public static String COLFAMILY = "HBASE_COLFAMLIY";
    public static String COL_1 = "HBASE_COL_1";
    public static String COL_2 = "HBASE_COL_2";
}


//~ Formatted by Jindent --- http://www.jindent.com
