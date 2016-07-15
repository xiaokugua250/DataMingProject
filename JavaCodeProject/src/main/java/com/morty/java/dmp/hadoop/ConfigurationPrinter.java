package com.morty.java.dmp.hadoop;
/**
 * Created by duliang on 2016/6/19.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * 打印输出所有configuration中的配置属性
 * Created by IntelliJ IDEA.
 * User: duliang
 * Date: 2016/6/19
 * Time: 16:42
 * email:duliang1128@163.com
 */
public class ConfigurationPrinter extends Configured implements Tool {
    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("mapred-default.xml");
        Configuration.addDefaultResource("mapred-site.xml");
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            System.out.println("entry = " + "entry.key===>" + entry.getKey() +
                    "entry.value====>" + entry.getValue());

        }
        return 0;
    }
}
