package com.morty.java.dmp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.CountVectorizer;
import org.apache.spark.ml.feature.CountVectorizerModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

/**
 * Created by duliang on 2016/6/12.
 */
public class SparkCountVector {
    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;

    public static void main(String[] args) {
        SparkCountVector sparkWord2Vec = new SparkCountVector();
        sparkWord2Vec.AlgthRun();
    }

    public void init() {
        // TODO: 2016/6/12  init
        conf = new SparkConf().setAppName("JavaTfIdfExample");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);

    }

    public JavaRDD<Row> getJavaRdd() {
        // Step 1  TODO: 2016/6/12 get javardd
        JavaRDD<Row> jrd = jsc.parallelize(Arrays.asList(
                RowFactory.create(Arrays.asList("a", "b", "c")),
                RowFactory.create(Arrays.asList("a", "b", "b", "c", "a"))
        ));
        return jrd;

    }

    public StructType getSchema() {
        // Step2 TODO: 2016/6/12  get scheam
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, true), false, Metadata.empty())
        });
        return schema;
    }

    public DataFrame getDataFrameBySQL(JavaRDD<Row> jrdd, StructType schema) {
        //  step3 TODO: 2016/6/12  get dataframe
        DataFrame documentDF = sqlContext.createDataFrame(jrdd, schema);
        return documentDF;
    }

    public CountVectorizerModel getCountVectoreModel(String input, String output, DataFrame dataFrame, String... params) {
        // step4 TODO: 2016/6/12
        // fit a CountVectorizerModel from the corpus
        CountVectorizerModel cvModel = new CountVectorizer()
                .setInputCol("text")
                .setOutputCol("feature")
                .setVocabSize(3)
                .setMinDF(2)
                .fit(dataFrame);
        return cvModel;

    }

    public CountVectorizerModel getCountVectoreModelByPriV(String[] vocabulary, String input, String output, DataFrame dataFrame, String... params) {
        // step5 TODO: 2016/6/12
        // alternatively, define CountVectorizerModel with a-priori vocabulary
        CountVectorizerModel cvm = new CountVectorizerModel(vocabulary)
                .setInputCol(input)
                .setOutputCol(output);

        return cvm;
    }

    public void showRes(CountVectorizerModel cvModel, DataFrame resultdf) {
        System.out.println("************start****************");
        resultdf.printSchema();

        cvModel.transform(resultdf).show();

        System.out.println("**************end**************");
    }

    public void AlgthRun() {
        init();
        DataFrame docDF = getDataFrameBySQL(getJavaRdd(), getSchema());
        showRes(getCountVectoreModel("text", "feature", docDF), docDF);
    }
}
