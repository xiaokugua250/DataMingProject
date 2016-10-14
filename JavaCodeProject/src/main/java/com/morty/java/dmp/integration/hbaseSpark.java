package com.morty.java.dmp.integration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by duliang on 2016/5/22.
 *
 * example shows how hbasecontext can be used to do a foreachPartition
 */
public class HBaseSpark {
    public SparkConf sparkConf;
    public JavaSparkContext jsc;

    //// TODO: 2016/5/22  init
    public void init() {
        jsc = new JavaSparkContext(sparkConf);
    }

    public void sparkHbase(final String tableName) {

        /*
         *  try {
         *    List<byte[]> list=new ArrayList<byte[]>();
         *    list.add(Bytes.toBytes("1"));
         *    list.add(Bytes.toBytes("2"));
         *    list.add(Bytes.toBytes("3"));
         *    list.add(Bytes.toBytes("4"));
         *    //.........
         *    list.add(Bytes.toBytes("5"));
         *    JavaRDD<byte[]> rdd=jsc.parallelize(list);
         *
         *    Configuration conf= HBaseConfiguration.create();
         *
         *    //// TODO: 2016/5/22  sparkhbasecontext dependies
         *    JavaHBaseContext hbaseContext=new JavaHbaseContext(jsc,conf);
         *
         *    hbaseContext.foreachPartition(rdd,new VoidFunction<Tuple2<Iterator<byte[]>,Connection>>(){
         *        public void call(Tuple2<Iterator<byte[]>, Connection> iteratorConnectionTuple2) throws Exception {
         *            Table table=iteratorConnectionTuple2._2().getTable(TableName.valueOf(tableName));
         *            BufferedMutator mutator=iteratorConnectionTuple2._2().getBufferedMutator(TableName.valueOf(tableName));
         *            while (iteratorConnectionTuple2._1().hasNext()){
         *                byte[] b=iteratorConnectionTuple2._1().next();
         *                Result r=table.get(new Get(b));
         *                if(r.getExists()){
         *                    mutator.mutate(new Put(b));
         *                }
         *            }
         *            mutator.flush();
         *            mutator.close();
         *            table.close();
         *        }
         *    });
         * } finally {
         *    jsc.stop();
         *
         * }
         */
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
