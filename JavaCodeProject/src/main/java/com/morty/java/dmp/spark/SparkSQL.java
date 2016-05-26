package com.morty.java.dmp.spark;

import com.morty.java.beans.User;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * spark sql 操作
 * Created by morty on 2016/05/25.
 */
public class SparkSQL {

    public JavaSparkContext javaSparkContext;
    public SparkConf sparkConf;
    public SQLContext sqlContext;
    public HiveContext hiveContext;
    Logger LOG = Logger.getLogger(SparkSQL.class.getName());

    public void init() {
        sparkConf = new SparkConf();

        javaSparkContext = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(javaSparkContext);

        hiveContext = new HiveContext(javaSparkContext);


    }


    /**
     * json方式获取dataframe
     *
     * @param jsonPath
     * @param type
     * @param javaJsonRDD
     * @param params
     * @return
     */
    public DataFrame getDFLoadJson(String jsonPath, String type, JavaRDD<String> javaJsonRDD, String... params) {
        // TODO: 2016/05/26 需检查测试
        if (type.equals("jsonrdd")) {
            DataFrame df = sqlContext.read().json(javaJsonRDD);
            return df;
        } else if (type.equals("jsonfile")) {
            DataFrame df = sqlContext.read().json(jsonPath);
            return df;
        }
        return null;
    }

    /**
     * 从其他数据源中获取dataframe
     *
     * @param tableUrl
     * @param type
     * @param tableName
     * @param column
     * @param perdicates
     * @param dbProperties
     * @param params
     * @return
     */
    public DataFrame getDFLoadDB(String tableUrl, String type, String tableName, String[] column, String[] perdicates, Properties dbProperties, String... params) {
        //// TODO: 2016/05/26  需测试检查
        if (type.equals("properties")) {
            DataFrame df = sqlContext.read().jdbc(tableUrl, tableName, dbProperties);
            return df;
        } else if (type.equals("colunms")) {
            DataFrame df = sqlContext.read().jdbc(tableUrl, tableName, column, dbProperties);
            return df;
        } else if (type.equals("perdicates")) {
            DataFrame df = sqlContext.read().jdbc(tableUrl, tableName, perdicates, dbProperties);
            return df;
        }
        return null;
    }

    /**
     * dataframe 操作
     * 参见 http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrame.html
     *
     * @param dataFrame
     */
    public void dataFrameOpt(DataFrame dataFrame) {

        dataFrame.schema();

        dataFrame.show();

        dataFrame.printSchema();

        dataFrame.columns();

        dataFrame.take(5);

        dataFrame.dropDuplicates();

        dataFrame.select(dataFrame.col("col_1"), dataFrame.col("col_2")).show();

        dataFrame.filter(dataFrame.col("col_1").geq("value_1")).show();

        dataFrame.groupBy(dataFrame.col("col_1")).count().show();

        dataFrame.where(dataFrame.col("col").geq("value")).show();

        dataFrame.alias("alias");

        dataFrame.collectAsList();

        dataFrame.intersect(dataFrame.agg(dataFrame.col("col_1"))).show();


    /*    dataFrame.where();

        dataFrame.agg();

        dataFrame.withColumnRenamed();

        dataFrame.toDF();

        dataFrame.select();

        dataFrame.toJSON();

        dataFrame.toJavaRDD();

        dataFrame.transform();*/


    }

    /**
     * @param sql
     * @param params
     * @return
     */
    public DataFrame getDFbySQL(String sql, String... params) {

        DataFrame dataFrame = sqlContext.sql(sql);
        return dataFrame;

    }

    /**
     * 通过反射方式创建dataframe
     *
     * @param beanFile
     * @param types
     * @param params
     * @return
     */
    public DataFrame rdd2DFbyReflection(String beanFile, String types, String... params) {
        JavaRDD<User> user = javaSparkContext.textFile(beanFile)
                .map(new Function<String, User>() {
                    @Override
                    public User call(String line) throws Exception {
                        String[] parts = line.split(",");

                        User user = new User();
                        user.setId(Integer.parseInt(parts[0]));
                        user.setName(parts[1]);
                        user.setAge(Integer.parseInt(parts[2]));
                        user.setIncome(Double.parseDouble(parts[3]));
                        user.setDescribe(parts[4]);
                        //....etc
                        return user;
                    }
                });

        // Apply a schema to an RDD of JavaBeans and register it as a table.
        DataFrame schemaUser = sqlContext.createDataFrame(user, User.class);
        return schemaUser;

        // TODO: 2016/05/26  可继续注册为临时表，继续采用SQL进行Dataframe操作
        // schemaUser.registerTempTable("user");

        /*
        *
        * SQL can be run over RDDs that have been registered as tables.
        DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
          public String call(Row row) {
            return "Name: " + row.getString(0);
          }
        }).collect();
        * */

    }

    /**
     * Create an RDD of Rows from the original RDD;
     * Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
     * Apply the schema to the RDD of Rows via createDataFrame method provided by SQLContext.
     *
     * @return
     */
    public DataFrame rdd2DFbyProgram(String beanFile, String types, String... params) {

        // Load a text file and convert each line to a JavaBean.
        JavaRDD<String> user = javaSparkContext.textFile(beanFile);

        // The schema is encoded in a string
        String schemaString = "id name age income describe";

        List<StructField> fields = new ArrayList<StructField>();
        for (String fieldName : schemaString.split(" ")) {
            fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows.
        JavaRDD<Row> rowRDD = user.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] fields = record.split(",");
                return RowFactory.create(fields[0], fields[1], fields[2], fields[3], fields[4]);
            }
        });

        DataFrame userDF = sqlContext.createDataFrame(rowRDD, schema);
        return userDF;

       /*
       // Register the DataFrame as a table.
       userDF.registerTempTable("user");
        DataFrame results = sqlContext.sql("SELECT name FROM people");

        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by ordinal.
        List<String> names = results.javaRDD().map(new Function<Row, String>() {
            public String call(Row row) {
                return "Name: " + row.getString(0);
            }
        }).collect();
        */

    }

    /**
     * 加载parquet数据类型
     *
     * @param parquetPath
     * @param params
     * @return
     */
    public DataFrame getParquetData(String parquetPath, String... params) {

        DataFrame dataFrame = sqlContext.read().load(parquetPath);
        return dataFrame;
    }

    /**
     * 保存dataframe
     *
     * @param dataFrame
     * @param target
     * @param params
     */
    public void saveDataFrame(DataFrame dataFrame, String target, String... params) {
        // TODO: 2016/05/26  请详细查看API操作，注如可设置保存mode 
        if (target.equals("parquet")) {
            dataFrame.write().parquet(target);
        }
    }

    /**
     * spark sql 操作hive
     *
     * @param sql
     */
    public void hiveDataOpt(String sql) {
        // TODO: 2016/05/26 注意hive环境配置 
        hiveContext = new HiveContext(sqlContext.sparkContext());
        hiveContext.sql(sql);
    }

    /**
     * jdbc 获取dataframe
     *
     * @param url
     * @param tableName
     * @param properties
     * @return
     */
    public DataFrame getDataByJdbc(String url, String tableName, Properties properties) {
        // TODO: 2016/05/26 注意jdbc配置 
        DataFrame jdbcDF = sqlContext.read().jdbc(url, tableName, properties);
        return jdbcDF;
    }



}
