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
 * spark �ı�������ȡ
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

    public void AlgthRun() {
        init();

        JavaRDD<Row> jrdd = getJavaRddTmpl();
        StructType schema = getStructTypeTmpl();
        DataFrame sentencedf = getDataframeBySQL(jrdd, schema);

        // TODO: 2016/6/12  ����model  getResDataframeByModel(sentencedf,);
        DataFrame resDataFrame = getResDataframeByModel(SparkInfo.NGRAM, sentencedf, "words", "ngrams", null);

        DFshow(resDataFrame);
    }

    public void DFshow(DataFrame resultDF, String... params) {
        System.out.println("resultDF = " + resultDF);
    }

    public void init() {
        conf = new SparkConf().setAppName("JavaTfIdfExample");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
    }

    public DataFrame getDataframeBySQL(JavaRDD<Row> javaRDD, StructType schema, String... params) {

        // TODO: ��ȡ
        DataFrame resDataframe = sqlContext.createDataFrame(javaRDD, schema);

        return resDataframe;
    }

    public JavaRDD<Row> getJavaRddTmpl() {

        // step 1 TODO: 2016/6/12  ����rdd
        JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(0, "I wish Java could use case classes"),
                RowFactory.create(1, "Logistic regression models are neat")));

        return jrdd;
    }

    public DataFrame getResDataframeByModel(Integer TRANSFORMER_YPE, DataFrame rawDataframe, String input,
                                            String output, String... parmas) {
        DataFrame resDataFrame = null;

        switch (TRANSFORMER_YPE) {
            case SparkInfo.NGRAM:
                NGram ngramTransformer = new NGram().setInputCol(input).setOutputCol(output);

                resDataFrame = ngramTransformer.transform(rawDataframe);

                break;

            case SparkInfo.BINARIZER:
                Binarizer binarizer = new Binarizer().setInputCol(input)
                        .setOutputCol(output)
                        .setThreshold(Integer.valueOf(parmas[0]));

                resDataFrame = binarizer.transform(rawDataframe);

                break;

            case SparkInfo.PCA:

                // TODO: 2016/6/12  ���ɷݷ���
                PCAModel pca = new PCA().setInputCol(input)
                        .setOutputCol(output)
                        .setK(Integer.valueOf(parmas[0]))
                        .fit(rawDataframe);

                resDataFrame = pca.transform(rawDataframe);

                break;

            case SparkInfo.POLYNOMIALEXPRASSION:

                // TODO: 2016/6/12 ����ʽ��չ
                PolynomialExpansion polyExpansion = new PolynomialExpansion().setInputCol(input)
                        .setOutputCol(output)
                        .setDegree(Integer.valueOf(parmas[0]));

                resDataFrame = polyExpansion.transform(rawDataframe);

                break;

            case SparkInfo.DCT:

                // TODO: 2016/6/12  ��ɢ���ұ任
                DCT dct = new DCT().setInputCol(input).setOutputCol(output).setInverse(Boolean.valueOf(parmas[0]));

                resDataFrame = dct.transform(rawDataframe);

                break;

            case SparkInfo.STRINGINDEXER:

                // TODO: 2016/6/12  Stringindexer
                StringIndexer indexer = new StringIndexer().setInputCol(input)
                        .setOutputCol(output)
                        .setHandleInvalid("skip");

                resDataFrame = indexer.fit(rawDataframe).transform(rawDataframe);

                break;

            case SparkInfo.INDEXTOSTRING:
                IndexToString converter = new IndexToString().setInputCol(input).setOutputCol(output);

                resDataFrame = converter.transform(rawDataframe);

                break;

            case SparkInfo.ONEHOTENCODEER:

                // TODO: 2016/6/12  ���ȱ���
                // http://blog.csdn.net/google19890102/article/details/44039761
                OneHotEncoder encoder = new OneHotEncoder().setInputCol(input).setOutputCol(output);

                resDataFrame = encoder.transform(rawDataframe);

                break;

            case SparkInfo.VECTORINDEXER:

                // TODO: 2016/6/12  vectorindexer ������
                VectorIndexer vectorindexer = new VectorIndexer().setInputCol(input)
                        .setOutputCol(output)
                        .setMaxCategories(Integer.valueOf(parmas[0]));

                resDataFrame = vectorindexer.fit(rawDataframe).transform(rawDataframe);

                break;

            case SparkInfo.NORMAILIZER:

                // TODO: 2016/6/12  ��һ��
                // http://www.fuqingchuan.com/2015/03/643.html
                Normalizer normalizer = new Normalizer().setInputCol(input)
                        .setOutputCol(output)
                        .setP(Double.valueOf(parmas[0]));

                resDataFrame = normalizer.transform(rawDataframe);

                break;

            case SparkInfo.STANDARDSCALER:

                // TODO: 2016/6/12  ��׼��
                // http://www.fuqingchuan.com/2015/03/643.html
                StandardScaler scaler = new StandardScaler().setInputCol(input)
                        .setOutputCol(output)
                        .setWithStd(true)
                        .setWithMean(false);

                resDataFrame = scaler.fit(rawDataframe).transform(rawDataframe);

                break;

            case SparkInfo.MINMAXSCALER:

                // TODO: 2016/6/12  �����С��׼��
                MinMaxScaler minMscaler = new MinMaxScaler().setInputCol(input).setOutputCol(output);

                resDataFrame = minMscaler.fit(rawDataframe).transform(rawDataframe);

                break;

            case SparkInfo.BUCKERTIZER:

                // TODO: 2016/6/12 Ͱ��׼��
                double[] splits = {Double.NEGATIVE_INFINITY, -0.5, 0.0, 0.5, Double.POSITIVE_INFINITY};
                Bucketizer bucketizer = new Bucketizer().setInputCol(input).setOutputCol(output).setSplits(splits);

                resDataFrame = bucketizer.transform(rawDataframe);

                break;

            case SparkInfo.ELEMNETTWISEPRODUCT:

                // TODO: 2016/6/12  Ȩ�� ������
                Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);
                ElementwiseProduct transformer = new ElementwiseProduct().setScalingVec(transformingVector)
                        .setInputCol(input)
                        .setOutputCol(output);

                resDataFrame = transformer.transform(rawDataframe);

                break;

            case SparkInfo.SQLTRANSFORMER:

                // TODO: 2016/6/12   SQL

            /*
             * SQLTransformer sqlTrans = new SQLTransformer().setStatement(
             *       "SELECT *, (v1 + v2) AS v3, (v1 * v2) AS v4 FROM __THIS__");
             */
                SQLTransformer sqlTrans = new SQLTransformer().setStatement(parmas[0]);

                resDataFrame = sqlTrans.transform(rawDataframe);

                break;

            case SparkInfo.VECTORASSERMBLER:

                // TODO: 2016/6/12
                // VectorAssembler is a transformer that combines a given list of columns into a single vector column

            /*
             * VectorAssembler assembler = new VectorAssembler()
             *       .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
             *       .setOutputCol("features");
             */
                VectorAssembler assembler = new VectorAssembler().setInputCols(parmas).setOutputCol(output);

                resDataFrame = assembler.transform(rawDataframe);

                break;

            case SparkInfo.QUANLITIEDISCRETIZER:

                // TODO: 2016/6/12  QuantileDiscrtizer
                QuantileDiscretizer discretizer = new QuantileDiscretizer().setInputCol(input)
                        .setOutputCol(output)
                        .setNumBuckets(Integer.valueOf(parmas[0]));

                resDataFrame = discretizer.fit(rawDataframe).transform(rawDataframe);

                break;

            //// TODO: 2016/6/12  ����ѡ����
            // �μ�http://lxw1234.com/archives/2016/03/619.htm
            // *****************************************************************************************
            case SparkInfo.VECTORSLICER:

                // TODO: 2016/6/12  VectorSlicer���ڴ�ԭ���������������и�һ���֣��γ��µ�����������
                // ���磬ԭ����������������Ϊ10������ϣ���и����е�5~10��Ϊ�µ�����������ʹ��VectorSlicer���Կ���ʵ��
                VectorSlicer vectorSlicer = new VectorSlicer().setInputCol(input).setOutputCol(output);

                vectorSlicer.setIndices(new int[]{1}).setNames(new String[]{"f3"});

                // or slicer.setIndices(new int[]{1, 2}), or slicer.setNames(new String[]{"f2", "f3"})
                resDataFrame = vectorSlicer.transform(rawDataframe);

                break;

            case SparkInfo.RFORMAULA:

                // TODO: 2016/6/12 RFormula���ڽ������е��ֶ�ͨ��R���Ե�Model Formulaeת��������ֵ��������Ϊһ������������Double���͵�labe
                RFormula formula = new RFormula().setFormula(input).setFeaturesCol(output).setLabelCol(parmas[0]);

                resDataFrame = formula.fit(rawDataframe).transform(rawDataframe);

                break;

            case SparkInfo.CHISQSELECTOR:

                // TODO: 2016/6/12   ChiSqSelector����ʹ�ÿ���������ѡ����������ά��
                ChiSqSelector selector = new ChiSqSelector().setNumTopFeatures(1)
                        .setFeaturesCol(input)
                        .setLabelCol(output)
                        .setOutputCol(parmas[0]);

                resDataFrame = selector.fit(rawDataframe).transform(rawDataframe);

                break;
        }

        return resDataFrame;
    }

    /**
     * @param input
     * @param output
     * @param params
     * @return
     */
    public StopWordsRemover getStopWordRemoveTmpl(String input, String output, String... params) {

        // TODO: 2016/6/12  ͣ�ô�����
        StopWordsRemover stopWordsRemover = new StopWordsRemover();

        stopWordsRemover.setInputCol(input).setOutputCol(output);

        return stopWordsRemover;
    }

    public StructType getStructTypeTmpl(String... parmas) {

        // step2 TODO: 2016/6/12  ����SQL schema
        StructType schema = new StructType(new StructField[]{new StructField("label",
                DataTypes.DoubleType,
                false,
                Metadata.empty()),
                new StructField("sentence",
                        DataTypes.StringType,
                        false,
                        Metadata.empty())});

        return schema;
    }

    /**
     * �ִ�
     *
     * @param input
     * @param output
     * @param isRegex �Ƿ�����ִ� Ĭ�� false
     * @param params
     * @return
     */
    public Object getTokenizerTmpl(String input, String output, Boolean isRegex, String pattern, String... params) {

        // step4 TODO: 2016/6/12 ���÷��� tokeniZer
        if (isRegex != true) {
            Tokenizer tokenizer = new Tokenizer();

            tokenizer.setInputCol(input).setOutputCol(output);

            return tokenizer;
        } else {
            RegexTokenizer regexTokenizer = new RegexTokenizer().setInputCol(input)
                    .setOutputCol(output)
                    .setPattern(pattern);

            return regexTokenizer;
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
