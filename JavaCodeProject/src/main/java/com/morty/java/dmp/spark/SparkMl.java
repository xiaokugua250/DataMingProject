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
 * Spark机器学习 ml包
 * Created by duliang on 2016/6/6.
 */
public class SparkMl {

    // TODO: 2016/6/6 spark机器学习
    /*
    * 1、获取训练数据
    * 2、运用算法
    * 3、设置算法参数
    *
    * */
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

    // TODO: 2016/6/6 获取训练数据
    public DataFrame getTrainData() {
        // Prepare training data.
        // We use LabeledPoint, which is a JavaBean.  Spark SQL can convert RDDs of JavaBeans
        // into DataFrames, where it uses the bean metadata to infer the schema.
        DataFrame training = sqlContext.createDataFrame(Arrays.asList(
                new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
                new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0)),
                new LabeledPoint(0.0, Vectors.dense(2.0, 1.3, 1.0)),
                new LabeledPoint(1.0, Vectors.dense(0.0, 1.2, -0.5))
        ), LabeledPoint.class);
        return training;
    }

    public Object getEstimator(String... params) {
        // TODO: 2016/6/6  创建算法实例，该实例为estimator
        LogisticRegression logisticRegression = new LogisticRegression();
        System.out.println("logisticRegression.explainParams() = " + logisticRegression.explainParams() + "\n");
        return logisticRegression;
    }

    public void AlgthormRun(Estimator estimator, DataFrame dataFrame, String... params) {
        // TODO: 2016/6/6  设置算法运行参数,并运行模型
        LogisticRegression logisticRegression = new LogisticRegression();
        LogisticRegressionModel logisticRegressionModel = logisticRegression.fit(dataFrame);

        // LogisticRegressionModel model= (LogisticRegressionModel) estimator.fit(dataFrame);

   /*LogisticRegressionModel logisticRegressionModel=logisticRegression.fit(dataFrame);
      Since model1 is a Model (i.e., a Transformer produced by an Estimator),
 we can view the parameters it used during fit().
 This prints the parameter (name: value) pairs, where names are unique IDs for this
 LogisticRegression instance.
System.out.println("Model 1 was fit using parameters: " + model1.parent().extractParamMap());
     */

    }

    public ParamMap getParmMap() {
        // TODO: 2016/6/6  通过paramMap指定特定参数
          /*
           We may alternatively specify parameters using a ParamMap.
    ParamMap paramMap = new ParamMap()
      .put(lr.maxIter().w(20)) // Specify 1 Param.
      .put(lr.maxIter(), 30) // This overwrites the original maxIter.
      .put(lr.regParam().w(0.1), lr.threshold().w(0.55)); // Specify multiple Params.

    // One can also combine ParamMaps.
    ParamMap paramMap2 = new ParamMap()
      .put(lr.probabilityCol().w("myProbability")); // Change output column name
    ParamMap paramMapCombined = paramMap.$plus$plus(paramMap2);
    */
        ParamMap paramMap = new ParamMap();
        // TODO: 2016/6/6  设置param
        return paramMap;


    }

    public void AlgthormRun2(DataFrame training, ParamMap paramMapCombined) {
        // Now learn a new model using the paramMapCombined parameters.
        // paramMapCombined overrides all parameters set earlier via lr.set* methods.
        // TODO: 2016/6/6  通过paramed参数运行模型
        LogisticRegression lr = new LogisticRegression();

        LogisticRegressionModel model2 = lr.fit(training, paramMapCombined);
        System.out.println("Model 2 was fit using parameters: " + model2.parent().extractParamMap());

    }

    public void makePrediction() {
        // TODO: 2016/6/6  预测
        /*
       // Make predictions on test documents using the Transformer.transform() method.
// LogisticRegression.transform will only use the 'features' column.
// Note that model2.transform() outputs a 'myProbability' column instead of the usual
// 'probability' column since we renamed the lr.probabilityCol parameter previously.
DataFrame results = model2.transform(test);
for (Row r: results.select("features", "label", "myProbability", "prediction").collect()) {
  System.out.println("(" + r.get(0) + ", " + r.get(1) + ") -> prob=" + r.get(2)
      + ", prediction=" + r.get(3));
}
*/
    }

    /**
     * 返回PipeLineState
     *
     * @param stages
     * @return
     */
    public Pipeline getPipeLine(PipelineStage... stages) {
        // TODO: 2016/6/11 设置pipelinestage
        Pipeline pipeline = new Pipeline();

        pipeline.setStages(stages);
        /*
        * Pipeline pipeline = new Pipeline()
         .setStages(new PipelineStage[] {tokenizer, hashingTF, lr});
        * */
        return pipeline;
    }

    /**
     * 返回parammap
     *
     * @param param
     * @param values
     * @return
     */
    public ParamMap[] getParamMaps(Param<Integer> param, Iterable<Integer> values) {
        // TODO: 2016/6/11 依据要求设置parammap
        /*
            * // We use a ParamGridBuilder to construct a grid of parameters to search over.
            // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
            // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
            ParamMap[] paramGrid = new ParamGridBuilder()
            .addGrid(hashingTF.numFeatures(), n ew int[]{10, 100, 1000})
            .addGrid(lr.regParam(), new double[]{0.1, 0.01})
            .build();
        * */
        ParamMap[] paramGrid = new ParamGridBuilder()
                .addGrid(param, values)
                .build();
        return paramGrid;
    }


    /**
     * 返回交叉验证crossvalidator
     *
     * @param pipeLine
     * @param evaluator
     * @param paramMap
     * @param folds
     * @param params
     * @return
     */
    public CrossValidator getCrossValidator(Pipeline pipeLine, Evaluator evaluator, ParamMap[] paramMap, int folds, String... params) {
        // TODO: 2016/6/11  设置crossvalidator 
       /*
        // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
     // This will allow us to jointly choose parameters for all Pipeline stages.
     // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
     // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
     // is areaUnderROC.
       * */
        CrossValidator crossvalidator = new CrossValidator();
        crossvalidator
                .setEstimator(pipeLine)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramMap)
                .setNumFolds(folds);
        return crossvalidator;

    }

    /**
     * 返回crossvalidatormodel
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
     * 进行预测
     *
     * @param crossValidatorModel
     * @param testDf
     */
    public void PredictByCrossValidator(CrossValidatorModel crossValidatorModel, DataFrame testDf) {
        // TODO: 2016/6/11
        // Make predictions on test documents. cvModel uses the best model found (lrModel).
        DataFrame predictions = crossValidatorModel.transform(testDf);

    }


    /**
     * 用trainValidationSplit
     *
     * @param estimator
     * @param evaluator
     * @param paramMap
     * @param ratio
     * @param params
     * @return
     */
    public TrainValidationSplit getTrainValidationSplit(Estimator estimator, Evaluator evaluator, ParamMap[] paramMap, Double ratio, String... params) {
        // TODO: 2016/6/12  设置参数
        /*
        // In this case the estimator is simply the linear regression.
        // A TrainValidationSplit requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
        * */
        TrainValidationSplit trainValidationSplit = new TrainValidationSplit();
        trainValidationSplit
                .setEstimator(estimator)
                .setEvaluator(evaluator)
                .setEstimatorParamMaps(paramMap)
                .setTrainRatio(ratio);
        return trainValidationSplit;

    }

    /**
     * 获取TrainValidationModel
     *
     * @param trainValidationSplit
     * @param dataframe
     * @return
     */
    public TrainValidationSplitModel getTrainValidationSplitMode(TrainValidationSplit trainValidationSplit, DataFrame dataframe) {
        // TODO: 2016/6/12  训练数据获取model
        TrainValidationSplitModel trainValidationModel = trainValidationSplit.fit(dataframe);
        return trainValidationModel;
    }

    /**
     * 采用trainValidationSplit 预测
     *
     * @param trainValidationModel
     * @param testdf
     * @param params
     */
    public void predictByTrainValidationSplit(TrainValidationSplitModel trainValidationModel, DataFrame testdf, String... params) {
        // TODO: 2016/6/12  进行预测
        /* Make predictions on test data. model is the model with combination of parameters
             that performed best.
        */
        trainValidationModel.transform(testdf);

    }


}


