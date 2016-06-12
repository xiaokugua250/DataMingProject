package com.morty.java.dmp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.*;

import java.util.Arrays;

/**
 * Created by duliang on 2016/6/12.
 */
public class SparkWord2Vec {
    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;

    public static void main(String[] args) {
        SparkWord2Vec sparkWord2Vec = new SparkWord2Vec();
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
                RowFactory.create(Arrays.asList("Hi I heard about Spark".split(" "))),
                RowFactory.create(Arrays.asList("I wish Java could use case classes".split(" "))),
                RowFactory.create(Arrays.asList("Logistic regression models are neat".split(" ")))
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

    public Word2Vec getWord2Vec() {
        // step4 TODO: 2016/6/12 getword2vec
        // Learn a mapping from words to Vectors.
        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("text")
                .setOutputCol("result")
                .setVectorSize(3)
                .setMinCount(0);
        return word2Vec;
    }

    public Word2VecModel getWord2VecModel(Word2Vec word2Vec, DataFrame documentDF) {
        //  step 5 TODO: 2016/6/12  get word2vecmodel
        Word2VecModel model = word2Vec.fit(documentDF);
        return model;
    }

    public DataFrame getResDataFrame(Word2VecModel model, DataFrame documentDF) {
        //  step 6TODO: 2016/6/12  get result
        DataFrame result = model.transform(documentDF);
        return result;
    }

    public void showRes(DataFrame resultdf) {
        System.out.println("************start****************");
        resultdf.printSchema();
        for (Row r : resultdf.select("result").take(3)) {
            System.out.println("result====>" + r);
        }
        System.out.println("**************end**************");
    }

    public void AlgthRun() {
        init();
        DataFrame docDF = getDataFrameBySQL(getJavaRdd(), getSchema());
        showRes(getResDataFrame(getWord2VecModel(getWord2Vec(), docDF), docDF));
    }
}
