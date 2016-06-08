package com.morty.java.dmp.spark;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

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

}


