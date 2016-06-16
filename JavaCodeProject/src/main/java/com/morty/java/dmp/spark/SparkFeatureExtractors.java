package com.morty.java.dmp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.*;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

/**
 * spark 文本特征提取
 * Created by duliang on 2016/6/12.
 */
public class SparkFeatureExtractors {

    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;

    public static void main(String[] args) {
        SparkFeatureExtractors sparkTFIDF = new SparkFeatureExtractors();

        sparkTFIDF.AlgthRun();
    }

    public void init() {
        conf = new SparkConf().setAppName("JavaTfIdfExample");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);

    }

    public JavaRDD<Row> getJavaRddTmpl() {
        //  step 1 TODO: 2016/6/12  返回rdd
        JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
                RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(0, "I wish Java could use case classes"),
                RowFactory.create(1, "Logistic regression models are neat")
        ));
        return jrdd;
    }

    public StructType getStructTypeTmpl(String... parmas) {
        // step2 TODO: 2016/6/12  设置SQL schema
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        return schema;
    }

    /**
     * 分词
     *
     * @param input
     * @param output
     * @param isRegex 是否正则分词 默认 false
     * @param params
     * @return
     */
    public Object getTokenizerTmpl(String input, String output, Boolean isRegex, String pattern, String... params) {
        // step4 TODO: 2016/6/12 设置返回 tokeniZer
        if (isRegex != true) {
            Tokenizer tokenizer = new Tokenizer();
            tokenizer
                    .setInputCol(input)
                    .setOutputCol(output);
            return tokenizer;

        } else {
            RegexTokenizer regexTokenizer = new RegexTokenizer()
                    .setInputCol(input)
                    .setOutputCol(output)
                    .setPattern(pattern);
            return regexTokenizer;
        }
    }

    /**
     * @param input
     * @param output
     * @param params
     * @return
     */
    public StopWordsRemover getStopWordRemoveTmpl(String input, String output, String... params) {
        // TODO: 2016/6/12  停用词设置
        StopWordsRemover stopWordsRemover = new StopWordsRemover();
        stopWordsRemover.setInputCol(input)
                .setOutputCol(output);
        return stopWordsRemover;
    }

    public DataFrame getDataframeBySQL(JavaRDD<Row> javaRDD, StructType schema, String... params) {
        // TODO: 获取
        DataFrame resDataframe = sqlContext.createDataFrame(javaRDD, schema);
        return resDataframe;
    }

    public DataFrame getResDataframeByModel(Integer TRANSFORMER_YPE, DataFrame rawDataframe, String input, String output, String... parmas) {
        DataFrame resDataFrame = null;
        switch (TRANSFORMER_YPE) {

            case SparkInfo.NGRAM:
                NGram ngramTransformer = new NGram().setInputCol(input).setOutputCol(output);
                resDataFrame = ngramTransformer.transform(rawDataframe);
                break;

            case SparkInfo.BINARIZER:

                Binarizer binarizer = new Binarizer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setThreshold(Integer.valueOf(parmas[0]));

                resDataFrame = binarizer.transform(rawDataframe);
                break;
            case SparkInfo.PCA:
                // TODO: 2016/6/12  主成份分析
                PCAModel pca = new PCA()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setK(Integer.valueOf(parmas[0]))
                        .fit(rawDataframe);
                resDataFrame = pca.transform(rawDataframe);
                break;

            case SparkInfo.POLYNOMIALEXPRASSION:
                // TODO: 2016/6/12 多项式扩展
                PolynomialExpansion polyExpansion = new PolynomialExpansion()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setDegree(Integer.valueOf(parmas[0]));
                resDataFrame = polyExpansion.transform(rawDataframe);
                break;
            case SparkInfo.DCT:
                // TODO: 2016/6/12  离散余弦变换
                DCT dct = new DCT()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setInverse(Boolean.valueOf(parmas[0]));
                resDataFrame = dct.transform(rawDataframe);
                break;

            case SparkInfo.STRINGINDEXER:
                // TODO: 2016/6/12  Stringindexer
                StringIndexer indexer = new StringIndexer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setHandleInvalid("skip");
                resDataFrame = indexer.fit(rawDataframe).transform(rawDataframe);
                break;
            case SparkInfo.INDEXTOSTRING:
                IndexToString converter = new IndexToString()
                        .setInputCol(input)
                        .setOutputCol(output);
                resDataFrame = converter.transform(rawDataframe);
                break;

            case SparkInfo.ONEHOTENCODEER:
                // TODO: 2016/6/12  独热编码
                // http://blog.csdn.net/google19890102/article/details/44039761
                OneHotEncoder encoder = new OneHotEncoder()
                        .setInputCol(input)
                        .setOutputCol(output);
                resDataFrame = encoder.transform(rawDataframe);
                break;

            case SparkInfo.VECTORINDEXER:
                // TODO: 2016/6/12  vectorindexer ？？？
                VectorIndexer vectorindexer = new VectorIndexer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setMaxCategories(Integer.valueOf(parmas[0]));
                resDataFrame = vectorindexer.fit(rawDataframe).transform(rawDataframe);
                break;
            case SparkInfo.NORMAILIZER:
                // TODO: 2016/6/12  归一化
                // http://www.fuqingchuan.com/2015/03/643.html
                Normalizer normalizer = new Normalizer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setP(Double.valueOf(parmas[0]));
                resDataFrame = normalizer.transform(rawDataframe);
                break;
            case SparkInfo.STANDARDSCALER:
                // TODO: 2016/6/12  标准化
                //http://www.fuqingchuan.com/2015/03/643.html
                StandardScaler scaler = new StandardScaler()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setWithStd(true)
                        .setWithMean(false);
                resDataFrame = scaler.fit(rawDataframe).transform(rawDataframe);
                break;
            case SparkInfo.MINMAXSCALER:
                // TODO: 2016/6/12  最大最小标准化
                MinMaxScaler minMscaler = new MinMaxScaler()
                        .setInputCol(input)
                        .setOutputCol(output);
                resDataFrame = minMscaler.fit(rawDataframe).transform(rawDataframe);
                break;

            case SparkInfo.BUCKERTIZER:
                // TODO: 2016/6/12 桶标准化
                double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};
                Bucketizer bucketizer = new Bucketizer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setSplits(splits);
                resDataFrame = bucketizer.transform(rawDataframe);
                break;

            case SparkInfo.ELEMNETTWISEPRODUCT:
                // TODO: 2016/6/12  权重 ？？？
                Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

                ElementwiseProduct transformer = new ElementwiseProduct()
                        .setScalingVec(transformingVector)
                        .setInputCol(input)
                        .setOutputCol(output);

                resDataFrame = transformer.transform(rawDataframe);
                break;

            case SparkInfo.SQLTRANSFORMER:
                // TODO: 2016/6/12   SQL
                /*SQLTransformer sqlTrans = new SQLTransformer().setStatement(
                        "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");*/
                SQLTransformer sqlTrans = new SQLTransformer().setStatement(
                        parmas[0]);
                resDataFrame = sqlTrans.transform(rawDataframe);
                break;
            case SparkInfo.VECTORASSERMBLER:
                // TODO: 2016/6/12
                // VectorAssembler is a transformer that combines a given list of columns into a single vector column
                /*VectorAssembler assembler = new VectorAssembler()
                        .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
                        .setOutputCol("features");*/
                VectorAssembler assembler = new VectorAssembler()
                        .setInputCols(parmas)
                        .setOutputCol(output);
                resDataFrame = assembler.transform(rawDataframe);
                break;
            case SparkInfo.QUANLITIEDISCRETIZER:
                // TODO: 2016/6/12  QuantileDiscrtizer
                QuantileDiscretizer discretizer = new QuantileDiscretizer()
                        .setInputCol(input)
                        .setOutputCol(output)
                        .setNumBuckets(Integer.valueOf(parmas[0]));
                resDataFrame = discretizer.fit(rawDataframe).transform(rawDataframe);
                break;

            //// TODO: 2016/6/12  特征选择器
            //参见http://lxw1234.com/archives/2016/03/619.htm
            //*****************************************************************************************
            case SparkInfo.VECTORSLICER:
                // TODO: 2016/6/12  VectorSlicer用于从原来的特征向量中切割一部分，形成新的特征向量，
                // 比如，原来的特征向量长度为10，我们希望切割其中的5~10作为新的特征向量，使用VectorSlicer可以快速实现
                VectorSlicer vectorSlicer = new VectorSlicer()
                        .setInputCol(input).setOutputCol(output);
                vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});
                // or slicer.setIndices(new int[]{1, 2}), or slicer.setNames(new String[]{"f2", "f3"})
                resDataFrame = vectorSlicer.transform(rawDataframe);
                break;
            case SparkInfo.RFORMAULA:
                // TODO: 2016/6/12 RFormula用于将数据中的字段通过R语言的Model Formulae转换成特征值，输出结果为一个特征向量和Double类型的labe
                RFormula formula = new RFormula()
                        .setFormula(input)
                        .setFeaturesCol(output)
                        .setLabelCol(parmas[0]);
                resDataFrame = formula.fit(rawDataframe).transform(rawDataframe);
                break;
            case SparkInfo.CHISQSELECTOR:
                // TODO: 2016/6/12   ChiSqSelector用于使用卡方检验来选择特征（降维）
                ChiSqSelector selector = new ChiSqSelector()
                        .setNumTopFeatures(1)
                        .setFeaturesCol(input)
                        .setLabelCol(output)
                        .setOutputCol(parmas[0]);
                resDataFrame = selector.fit(rawDataframe).transform(rawDataframe);
                break;

        }
        return resDataFrame;
    }

    public void DFshow(DataFrame resultDF, String... params) {
        System.out.println("resultDF = " + resultDF);
    }

    public void AlgthRun() {
        init();
        JavaRDD<Row> jrdd = getJavaRddTmpl();
        StructType schema = getStructTypeTmpl();
        DataFrame sentencedf = getDataframeBySQL(jrdd, schema);
        // TODO: 2016/6/12  调用model  getResDataframeByModel(sentencedf,);
        DataFrame resDataFrame = getResDataframeByModel(SparkInfo.NGRAM, sentencedf, "words", "ngrams", null);
        DFshow(resDataFrame);

    }
}
