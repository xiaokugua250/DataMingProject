package com.morty.java.dmp.Hbase;

/**
 * Created by IntelliJ IDEA.
 * Created by duliang on 2016/7/29.
 * Time: 17:14
 * email:duliang1128@163.com
 */


public class AnalyzeData {
/*
    static class AnalyzeMapper extends TableMapper<Text,IntWritable>{
        private JSONParser parser = new JSONParser();
        private IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(ImmutableBytesWritable row, Result colums, Context context) throws IOException, InterruptedException {
            context.getCounter(org.apache.hadoop.hbase.mapreduce.RowCounter.RowCounterMapper.Counters.ROWS).increment(1);
            String value = null;
            for(KeyValue kv : colums.list()){
                context.getCounter(org.apache.hadoop.hbase.mapreduce.RowCounter.RowCounterMapper.Counters.COLS).increment(1);
                value = Bytes.toStringBinary(kv.getValue());
                try {
                    JSONObject json= (JSONObject) parser.parse(value);
                    String author = (String) json.get("author");
                    context.write(Counters.VALID).increment(1);
                } catch (ParseException e) {
                    e.printStackTrace();
                    System.err.print("row"+Bytes.toStringBinary(row.get())+",json"+value);
                    context.getCounter(Counters.ERROR).increment(1);
                }
            }

        }

        static class AnalyzeReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

                int count = 0;
                for(IntWritable one : values) count++;
                context.write(key,new IntWritable(count));
            }
        }

        public static void main(String[] args) {
            // TODO: 2016/7/29  get scan ,table,column,
            Scan scan = new Scan();

            if(column != null){
                byte[][] colKey =  KeyValue.parseColumn(Bytes.toBytes(column));
                if(colKey.length > 1){
                    scan.addColumn(colKey[0],colKey[1]);

                }else {
                    scan.addColumn(colKey[0]);
                }
            }

            Job job = new Job(conf,"Analyze data in "+ table);
            job.setJarByClass(AnalyzeData.class);
            // TODO: 2016/7/29  table
            TableMapReduceUtil.initTableMapperJob(table,
                    scan,AnalyzeMapper.class,Text.class,IntWritable.class,job);
            job.setReducerClass(AnalyzeReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            FileOutputFormat.setOutputPath(job,new Path(output));
            // TODO: 2016/7/29   System.exit(job.waitForCompletion(true) ? 0 :1);
        }
    }*/
}
