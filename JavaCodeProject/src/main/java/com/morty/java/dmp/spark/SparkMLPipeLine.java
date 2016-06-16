package com.morty.java.dmp.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by duliang on 2016/6/6.
 */
public class SparkMLPipeLine {

    public JavaSparkContext javaSparkContext;
    public SparkConf sparkConf;
    public SQLContext sqlContext;
    Logger LOG = Logger.getLogger(SparkSQL.class.getName());


    public void init() {
        // TODO: 2016/6/6 算法初始化
        sparkConf = new SparkConf();

        javaSparkContext = new JavaSparkContext(sparkConf);

        sqlContext = new SQLContext(javaSparkContext);


    }

    public void SetPipeLine() {
        // TODO: 2016/6/6  设置pipeline操作

// Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text")
                .setOutputCol("words");
        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000)
                .setInputCol(tokenizer.getOutputCol())
                .setOutputCol("features");
        LogisticRegression lr = new LogisticRegression()
                .setMaxIter(10)
                .setRegParam(0.01);
        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

    }

    public void PipeLineRun(Pipeline pipeline, DataFrame dataframe) {
        // TODO: 2016/6/6  运行pipeline，训练模型

        PipelineModel pipelineModel = pipeline.fit(dataframe);

    }
// Fit the pip

    public void predict(PipelineModel pipelineModel, DataFrame dataframe) {
        // TODO: 2016/6/6  运行模型预测

        pipelineModel.transform(dataframe);

    }

}
