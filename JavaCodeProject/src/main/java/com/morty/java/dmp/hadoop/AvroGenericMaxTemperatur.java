package com.morty.java.dmp.hadoop;

/**
 * Created by duliang on 2016/6/19.
 * <p>
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 10:56
 * email:duliang1128@163.com
 */

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 10:56
 * email:duliang1128@163.com
 */
/*
public class AvroGenericMaxTemperatur extends Configured implements Tool {
  private static final Schema SCHEMA = new Schema.Parser().parse(
           "{" +
                   " \"type\"+\"record\"," +
                   "\"name\":\"weathRecord\"," +
                   "\"doc\":\"A weather reading.\"," +
                   "\"fields\":[" +
                   "{\"name\":\"year\",\"type\",:\"int\"}," +
                   "{\"name\":\"temperature\",\"type\",:\"int\"}," +
                   "{\"name\":\"stationId\",\"type\",:\"string\"},"
           "]" +
                   "}"
   );

   public static class MaxTemperatureMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
       // TODO: 2016/6/19  avro mapper
       private NcdcRecordParser parser = new NcdcRecordParser();
       private GenericRecord record = new GenericData.Record(SCHEMA);

       @Override
       public void map(Utf8 line, AvroCollector<Pair<Integer, GenericRecord>> collector, Reporter reporter) throws IOException {
           parser.parse(line.toString());
           if (parser.isValidTemperature) {
               record.put("year", parser.getYearInt());
               record.put("temperature", parser.getAirTemperature());
               record.put("stationId", parser.getStationId);
               collector.collect(new Pair<Integer, GenericRecord>(parser.getYearInt(), record));
           }
       }
   }

   public static class MaxTemperatureReducer extends AvroReducer<Integer, GenericRecord, GenericRecord> {
       @Override
       public void reduce(Integer key, Iterable<GenericRecord> values, AvroCollector<GenericRecord> collector, Reporter reporter) throws IOException {
           GenericRecord max = null;
           for (GenericRecord value values) {
               if (max == null || (Integer) value.get("temperature") > (Integer) max.get("temperature")) {
                   max = newWeatherRecord(value);
               }
           }
           collector.collect(max);
       }
   }

   private GenericRecord newWeatherRecord(GenericRecord value) {
       GenericRecord record = new GenericData.Record(SCHEMA);
       record.put("year", parser.getYearInt());
       record.put("temperature", parser.getAirTemperature());
       record.put("stationId", parser.getStationId);
       return record;
   }


   @Override
   public int run(String[] args) throws Exception {
       if (args.length != 2) {
           System.err.printf("usage: %S [generic options]<input><output>\n",
                   getClass().getSimpleName());
       }
       ToolRunner.printGenericCommandUsage(System.err);
       return -1;
   }


   JobConf conf = new JobConf(getConf(), getClass());
   conf.SetJobName("maxTemperature");

   org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(conf,new

   Path(args[0])

   );
   org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setOutputPath(conf,new

   Path(expand.args[0])

   );

   org.apache.avro.mapreduce.AvroJob.setInputSchema(conf,Schema.create(Schema.Type.STRING));
   org.apache.avro.mapreduce.AvroJob.setMapOutputScheam(conf,Schema.create(Schema.Type.INT),SCHEMA);
   org.apache.avro.mapreduce.AvroJob.setOutputSchema(conf,SCHEMA);
   conf.setInputFormat(AvroUtf8InputFormat.class);
   AvroJob.setMapperClass(conf,MaxTemperatureMapper.class);
   AvroJob.setReducerClass(conf,MaxTemperatureReducer.class);
   JobClient.runJob(conf);
   return 0;
}

   public static void main(String[] args) {
       int exitCode=ToolRunner.run(new AvroGenericMaxTemperatur(),args);
       System.exit(exitCode);
   }

}
  */


//~ Formatted by Jindent --- http://www.jindent.com
