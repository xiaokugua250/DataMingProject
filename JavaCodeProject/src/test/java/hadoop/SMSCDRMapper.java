package hadoop;

/**
 * Created by duliang on 2016/6/19.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 17:29
 * email:duliang1128@163.com
 */
public class SMSCDRMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable addOne = new IntWritable(1);
    private Text status = new Text();

    /**
     * Returns the SMS status code and its count
     */
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws java.io.IOException, InterruptedException {

        // 655209;1;796764372490213;804422938115889;6 is the Sample record format
        String[] line = value.toString().split(";");

        // If record is of SMS CDR
        if (Integer.parseInt(line[1]) == 1) {
            status.set(line[4]);
            context.write(status, addOne);
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
