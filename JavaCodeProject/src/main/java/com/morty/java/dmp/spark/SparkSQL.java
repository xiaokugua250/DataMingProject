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
 * spark sql ����
 * Created by morty on 2016/05/25.
 */
public class SparkSQL {
    public JavaSparkContext javaSparkContext;
    public SparkConf sparkConf;
    public SQLContext sqlContext;
    public HiveContext hiveContext;
    Logger LOG = Logger.getLogger(SparkSQL.class.getName());

    /**
     * dataframe ����
     * �μ� http://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrame.html
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

        /*
         *     dataFrame.where();
         *
         *   dataFrame.agg();
         *
         *   dataFrame.withColumnRenamed();
         *
         *   dataFrame.toDF();
         *
         *   dataFrame.select();
         *
         *   dataFrame.toJSON();
         *
         *   dataFrame.toJavaRDD();
         *
         *   dataFrame.transform();
         */
    }

    /**
     * spark sql ����hive
     *
     * @param sql
     */
    public void hiveDataOpt(String sql) {

        // TODO: 2016/05/26 ע��hive��������
        hiveContext = new HiveContext(sqlContext.sparkContext());
        hiveContext.sql(sql);
    }

    public void init() {
        sparkConf = new SparkConf();
        javaSparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(javaSparkContext);
        hiveContext = new HiveContext(javaSparkContext);
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

                return RowFactory.create(fields[0],
                        fields[1],
                        fields[2],
                        fields[3],
                        fields[4]);
            }
        });
        DataFrame userDF = sqlContext.createDataFrame(rowRDD, schema);

        return userDF;

        /*
         * // Register the DataFrame as a table.
         * userDF.registerTempTable("user");
         * DataFrame results = sqlContext.sql("SELECT name FROM people");
         *
         * // The results of SQL queries are DataFrames and support all the normal RDD operations.
         * // The columns of a row in the result can be accessed by ordinal.
         * List<String> names = results.javaRDD().map(new Function<Row, String>() {
         *    public String call(Row row) {
         *        return "Name: " + row.getString(0);
         *    }
         * }).collect();
         */
    }

    /**
     * ͨ�����䷽ʽ����dataframe
     *
     * @param beanFile
     * @param types
     * @param params
     * @return
     */
    public DataFrame rdd2DFbyReflection(String beanFile, String types, String... params) {
        JavaRDD<User> user = javaSparkContext.textFile(beanFile).map(new Function<String, User>() {
            @Override
            public User call(String line) throws Exception {
                String[] parts = line.split(",");
                User user = new User();

                user.setId(Integer.parseInt(parts[0]));
                user.setName(parts[1]);
                user.setAge(Integer.parseInt(parts[2]));
                user.setIncome(Double.parseDouble(parts[3]));
                user.setDescribe(parts[4]);

                // ....etc
                return user;
            }
        });

        // Apply a schema to an RDD of JavaBeans and register it as a table.
        DataFrame schemaUser = sqlContext.createDataFrame(user, User.class);

        return schemaUser;

        // TODO: 2016/05/26  �ɼ���ע��Ϊ��ʱ����������SQL����Dataframe����
        // schemaUser.registerTempTable("user");

        /*
         *
         * SQL can be run over RDDs that have been registered as tables.
         * DataFrame teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
         *
         * // The results of SQL queries are DataFrames and support all the normal RDD operations.
         * // The columns of a row in the result can be accessed by ordinal.
         * List<String> teenagerNames = teenagers.javaRDD().map(new Function<Row, String>() {
         * public String call(Row row) {
         *   return "Name: " + row.getString(0);
         * }
         * }).collect();
         */
    }

    /**
     * ����dataframe
     *
     * @param dataFrame
     * @param target
     * @param params
     */
    public void saveDataFrame(DataFrame dataFrame, String target, String... params) {

        // TODO: 2016/05/26  ����ϸ�鿴API������ע������ñ���mode
        if (target.equals("parquet")) {
            dataFrame.write().parquet(target);
        }
    }

    /**
     * ���ܵ���
     */
    public void sqlPerformanceTunning(Properties properties) {

        /*
         * Spark SQL can cache tables using an in-memory columnar format by calling sqlContext.cacheTable("tableName") or dataFrame.cache(). Then Spark SQL will scan only required columns and will automatically tune compression to minimize memory usage and GC pressure. You can call sqlContext.uncacheTable("tableName") to remove the table from memory.
         * Configuration of in-memory caching can be done using the setConf method on SQLContext or by running SET key=value commands using SQL.
         *
         * Property Name   Default Meaning
         * spark.sql.inMemoryColumnarStorage.compressed    true    When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data.
         * spark.sql.inMemoryColumnarStorage.batchSize     10000   Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data.
         * Other Configuration Options
         * The following options can also be used to tune the performance of query execution. It is possible that these options will be deprecated in future release as more optimizations are performed automatically.
         *
         * Property Name   Default Meaning
         * spark.sql.autoBroadcastJoinThreshold    10485760 (10 MB)        Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. Note that currently statistics are only supported for Hive Metastore tables where the command ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan has been run.
         * spark.sql.tungsten.enabled      true    When true, use the optimized Tungsten physical execution backend which explicitly manages memory and dynamically generates bytecode for expression evaluation.
         * spark.sql.shuffle.partitions    200     Configures the number of partitions to use when shuffling data for joins or aggregations.
         */
        sqlContext.setConf(properties);
    }

    /**
     * ����������Դ�л�ȡdataframe
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
    public DataFrame getDFLoadDB(String tableUrl, String type, String tableName, String[] column, String[] perdicates,
                                 Properties dbProperties, String... params) {

        //// TODO: 2016/05/26  ����Լ��
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
     * json��ʽ��ȡdataframe
     *
     * @param jsonPath
     * @param type
     * @param javaJsonRDD
     * @param params
     * @return
     */
    public DataFrame getDFLoadJson(String jsonPath, String type, JavaRDD<String> javaJsonRDD, String... params) {

        // TODO: 2016/05/26 �������
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
     * @param sql
     * @param params
     * @return
     */
    public DataFrame getDFbySQL(String sql, String... params) {
        DataFrame dataFrame = sqlContext.sql(sql);

        return dataFrame;
    }

    /**
     * jdbc ��ȡdataframe
     *
     * @param url
     * @param tableName
     * @param properties
     * @return
     */
    public DataFrame getDataByJdbc(String url, String tableName, Properties properties) {

        // TODO: 2016/05/26 ע��jdbc����

        /*
         * Tables from the remote database can be loaded as a DataFrame or Spark SQL Temporary table using the Data Sources API. The following options are supported:
         *
         * Property Name   Meaning
         * url     The JDBC URL to connect to.
         * dbtable The JDBC table that should be read. Note that anything that is valid in a FROM clause of a SQL query can be used. For example, instead of a full table you could also use a subquery in parentheses.
         * driver  The class name of the JDBC driver to use to connect to this URL.
         * partitionColumn, lowerBound, upperBound, numPartitions  These options must all be specified if any of them is specified. They describe how to partition the table when reading in parallel from multiple workers. partitionColumn must be a numeric column from the table in question. Notice that lowerBound and upperBound are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned.
         * fetchSize       The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows).
         */
        DataFrame jdbcDF = sqlContext.read().jdbc(url, tableName, properties);

        return jdbcDF;
    }

    /**
     * ����parquet��������
     *
     * @param parquetPath
     * @param params
     * @return
     */
    public DataFrame getParquetData(String parquetPath, String... params) {
        DataFrame dataFrame = sqlContext.read().load(parquetPath);

        return dataFrame;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
