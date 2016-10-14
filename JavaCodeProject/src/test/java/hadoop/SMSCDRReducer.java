package hadoop;

/**
 * Created by duliang on 2016/6/19.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 17:31
 * email:duliang1128@163.com
 */
public class SMSCDRReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    protected void reduce(Text key, Iterable<IntWritable> values, Mapper.Context context)
            throws java.io.IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        context.write(key, new IntWritable(sum));
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
