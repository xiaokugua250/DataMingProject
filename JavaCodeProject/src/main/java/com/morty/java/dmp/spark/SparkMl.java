package com.morty.java.dmp.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.Evaluator;
import org.apache.spark.ml.param.Param;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.ml.tuning.*;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import scala.collection.Iterable;

import java.util.Arrays;

/**
 * Spark����ѧϰ ml��
 * Created by duliang on 2016/6/6.
 */
public class SparkMl {
    /*
     * 1����ȡѵ������
     * 2�������㷨
     * 3�������㷨����
     *
     */
    public JavaSparkContext javaSparkContext;

    // TODO: 2016/6/6 spark����ѧϰ
    public SparkConf sparkConf;
    public SQLContext sqlContext;
    Logger LOG = Logger.getLogger(SparkSQL.class.getName());

    public void AlgthormRun(Estimator estimator, DataFrame dataFrame, String... params) {

        // TODO: 2016/6/6  �����㷨���в���,������ģ��
        LogisticRegression logisticRegression = new LogisticRegression();
        LogisticRegressionModel logisticRegressionModel = logisticRegression.fit(dataFrame);

        // LogisticRegressionModel model= (LogisticRegressionModel) estimator.fit(dataFrame);

        /*
         * LogisticRegressionModel logisticRegressionModel=logisticRegression.fit(dataFrame);
         *  Since model1 is a Model (i.e., a Transformer produced by an Estimator),
         * we can view the parameters it used during fit().
         * This prints the parameter (name: value) pairs, where names are unique IDs for this
         * LogisticRegression instance.
         * System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());
         */
    }

    public void AlgthormRun2(DataFrame training, ParamMap paramMapCombined) {

        // Now learn a new model using the paramMapCombined parameters.
        // paramMapCombined overrides all parameters set earlier via lr.set* methods.
        // TODO: 2016/6/6  ͨ��paramed��������ģ��
        LogisticRegression lr = new LogisticRegression();
        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);

        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());
    }

    /**
     * ����Ԥ��
     *
     * @param crossValidatorModel
     * @param testDf
     */
    public void PredictByCrossValidator(CrossValidatorModel crossValidatorModel, DataFrame testDf) {

        // TODO: 2016/6/11
        // Make predictions on test documents. cvModel uses the best model found (lrModel).
        DataFrame predictions = crossValidatorModel.transform(testDf);
    }

    public void init() {

        // TODO: 2016/6/6 �㷨��ʼ��
        sparkConf = new SparkConf();
        javaSparkContext = new JavaSparkContext(sparkConf);
        sqlContext = new SQLContext(javaSparkContext);
    }

    public void makePrediction() {

        // TODO: 2016/6/6  Ԥ��

        /*
         * // Make predictions on test documents using the Transformer.transform() method.
         * / LogisticRegression.transform will only use the 'features' column.
         * / Note that model2.transform() outputs a 'myProbability' column instead of the usual
         * / 'probability' column since we renamed the lr.probabilityCol parameter previously.
         * DataFrame results = model2.transform(test);
         * for (Row r: results.select("features", "label", "myProbability", "prediction").collect()) {
         * System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
         * + ", prediction=" + r.get(3));
         * }
         */
    }

    /**
     * ����trainValidationSplit Ԥ��
     *
     * @param trainValidationModel
     * @param testdf
     * @param params
     */
    public void predictByTrainValidationSplit(TrainValidationSplitModel trainValidationModel, DataFrame testdf,
                                              String... params) {

        // TODO: 2016/6/12  ����Ԥ��

        /*
         *  Make predictions on test data. model is the model with combination of parameters
         *    that performed best.
         */
        trainValidationModel.transform(testdf);
    }

    /**
     * ����crossvalidatormodel
     *
     * @param crossValidator
     * @param df
     * @return
     */
    public CrossValidatorModel getCrossValdateModel(CrossValidator crossValidator, DataFrame df) {

        // TODO: 2016/6/11  // Run cross-validation, and choose the best set of parameters.
        CrossValidatorModel crossvalidateModel = crossValidator.fit(df);

        return crossvalidateModel;
    }

    /**
     * ���ؽ�����֤crossvalidator
     *
     * @param pipeLine
     * @param evaluator
     * @param paramMap
     * @param folds
     * @param params
     * @return
     */
    public CrossValidator getCrossValidator(Pipeline pipeLine, Evaluator evaluator, ParamMap[] paramMap, int folds,
                                            String... params) {

        // TODO: 2016/6/11  ����crossvalidator

        /*
         * // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
         * // This will allow us to jointly choose parameters for all Pipeline stages.
         * // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
         * // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
         * // is areaUnderROC.
         */
        CrossValidator crossvalidator = new CrossValidator();

        crossvalidator.setEstimator(pipeLine)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramMap)
                .setNumFolds(folds);

        return crossvalidator;
    }

    public Object getEstimator(String... params) {

        // TODO: 2016/6/6  �����㷨ʵ������ʵ��Ϊestimator
        LogisticRegression logisticRegression = new LogisticRegression();

        System.out.println("logisticRegression.explainParams() = " + logisticRegression.explainParams() + "\n");

        return logisticRegression;
    }

    /**
     * ����parammap
     *
     * @param param
     * @param values
     * @return
     */
    public ParamMap[] getParamMaps(Param<Integer> param, Iterable<Integer> values) {

        // TODO: 2016/6/11 ����Ҫ������parammap

        /*
         *    // We use a ParamGridBuilder to construct a grid of parameters to search over.
         *   // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
         *   // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
         *   ParamMap[] paramGrid = new ParamGridBuilder()
         *   .addGrid(hashingTF.numFeatures(), n ew int[]{10, 100, 1000})
         *   .addGrid(lr.regParam(), new double[]{0.1, 0.01})
         *   .build();
         */
        ParamMap[] paramGrid = new ParamGridBuilder().addGrid(param, values).build();

        return paramGrid;
    }

    public ParamMap getParmMap() {

        // TODO: 2016/6/6  ͨ��paramMapָ���ض�����

        /*
         * We may alternatively specify parameters using a ParamMap.
         * ParamMap paramMap = new ParamMap()
         * .put(lr.maxIter().w(20)) // Specify 1 Param.
         * .put(lr.maxIter(), 30) // This overwrites the original maxIter.
         * .put(lr.regParam().w(0.1), lr.threshold().w(0.55)); // Specify multiple Params.
         *
         * // One can also combine ParamMaps.
         * ParamMap paramMap2 = new ParamMap()
         * .put(lr.probabilityCol().w("myProbability")); // Change output column name
         * ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);
         */
        ParamMap paramMap = new ParamMap();

        // TODO: 2016/6/6  ����param
        return paramMap;
    }

    /**
     * ����PipeLineState
     *
     * @param stages
     * @return
     */
    public Pipeline getPipeLine(PipelineStage... stages) {

        // TODO: 2016/6/11 ����pipelinestage
        Pipeline pipeline = new Pipeline();

        pipeline.setStages(stages);

        /*
         * Pipeline pipeline = new Pipeline()
         * .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});
         */
        return pipeline;
    }

    // TODO: 2016/6/6 ��ȡѵ������
    public DataFrame getTrainData() {

        // Prepare training data.
        // We use LabeledPoint, which is a JavaBean.  Spark SQL can convert RDDs of JavaBeans
        // into DataFrames, where it uses the bean metadata to infer the schema.
        DataFrame training = sqlContext.createDataFrame(Arrays.asList(new LabeledPoint(1.0,
                        Vectors.dense(0.0, 1.1, 0.1)),
                new LabeledPoint(0.0,
                        Vectors.dense(2.0, 1.0, -1.0)),
                new LabeledPoint(0.0,
                        Vectors.dense(2.0, 1.3, 1.0)),
                new LabeledPoint(1.0,
                        Vectors.dense(0.0, 1.2, -0.5))),
                LabeledPoint.class);

        return training;
    }

    /**
     * ��trainValidationSplit
     *
     * @param estimator
     * @param evaluator
     * @param paramMap
     * @param ratio
     * @param params
     * @return
     */
    public TrainValidationSplit getTrainValidationSplit(Estimator estimator, Evaluator evaluator, ParamMap[] paramMap,
                                                        Double ratio, String... params) {

        // TODO: 2016/6/12  ���ò���

        /*
         * // In this case the estimator is simply the linear regression.
         * // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
         */
        TrainValidationSplit trainValidationSplit = new TrainValidationSplit();

        trainValidationSplit.setEstimator(estimator)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramMap)
                .setTrainRatio(ratio);

        return trainValidationSplit;
    }

    /**
     * ��ȡTrainValidationModel
     *
     * @param trainValidationSplit
     * @param dataframe
     * @return
     */
    public TrainValidationSplitModel getTrainValidationSplitMode(TrainValidationSplit trainValidationSplit,
                                                                 DataFrame dataframe) {

        // TODO: 2016/6/12  ѵ�����ݻ�ȡmodel
        TrainValidationSplitModel trainValidationModel = trainValidationSplit.fit(dataframe);

        return trainValidationModel;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
