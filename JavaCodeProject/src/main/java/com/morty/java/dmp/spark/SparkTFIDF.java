package com.morty.java.dmp.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.*;
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
 * Created by duliang on 2016/6/12.
 */
public class SparkTFIDF {
    private SparkConf conf;
    private JavaSparkContext jsc;
    private SQLContext sqlContext;

    public static void main(String[] args) {
        SparkTFIDF sparkTFIDF = new SparkTFIDF();

        sparkTFIDF.AlgthRun();
    }

    public void AlgthRun() {
        init();

        JavaRDD<Row> jrdd = getJavaRddTmpl();
        StructType schema = getStructTypeTmpl();
        DataFrame sentencedf = getSentenceDataFrameTmpl(jrdd, schema);
        Tokenizer tokenizer = (Tokenizer) getTokenizerTmpl("sentence", "words", false, null);
        DataFrame featuredf = getfeaturizedDataFrameTmpl(getHashingTFTmpl("words", "rawFeatures", 20),
                getWordDataFrameTmpl(tokenizer, sentencedf));
        DataFrame rescalData = getScaleData(getIdfModelTmpl(getIdfTmpl("rawFeatures", "features"), featuredf),
                featuredf);
    }

    public void DFshow(DataFrame resultDF, String... params) {
        System.out.println("resultDF = " + resultDF);
    }

    public DataFrame getfeaturizedDataFrameTmpl(HashingTF hashingTf, DataFrame wordDataFrame, String... params) {

        // TODO: 2016/6/12  �����ݻ�ȡ����ֵ
        DataFrame featuredDataframe = hashingTf.transform(wordDataFrame);

        return featuredDataframe;
    }

    public void init() {
        conf = new SparkConf().setAppName("JavaTfIdfExample");
        jsc = new JavaSparkContext(conf);
        sqlContext = new SQLContext(jsc);
    }

    public HashingTF getHashingTFTmpl(String input, String output, int numFeatures, String... params) {

        // TODO: 2016/6/12  ����hashingtf
        HashingTF hashingtf = new HashingTF();

        hashingtf.setInputCol(input).setOutputCol(output).setNumFeatures(numFeatures);

        return hashingtf;
    }

    public IDFModel getIdfModelTmpl(IDF idf, DataFrame featuredf, String... params) {

        // TODO: 2016/6/12  ��ȡidfmodel
        IDFModel idfModel = idf.fit(featuredf);

        return idfModel;
    }

    public IDF getIdfTmpl(String input, String output, String... params) {

        // TODO: 2016/6/12  ����idf
        IDF idf = new IDF();

        idf.setInputCol(input).setOutputCol(output);

        return idf;
    }

    public JavaRDD<Row> getJavaRddTmpl() {

        // step 1 TODO: 2016/6/12  ����rdd
        JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(RowFactory.create(0, "Hi I heard about Spark"),
                RowFactory.create(0, "I wish Java could use case classes"),
                RowFactory.create(1, "Logistic regression models are neat")));

        return jrdd;
    }

    public DataFrame getScaleData(IDFModel idfModel, DataFrame featureDate, String... parmas) {

        // TODO: 2016/6/12  ��һ������
        DataFrame scaleDataframe = idfModel.transform(featureDate);

        return scaleDataframe;
    }

    public DataFrame getSentenceDataFrameTmpl(JavaRDD<Row> rdd, StructType type, String... params) {

        // step3  TODO: 2016/6/12  ���÷���dataframe
        DataFrame dataframe = sqlContext.createDataFrame(rdd, type);

        return dataframe;
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

    public DataFrame getWordDataFrameTmpl(Tokenizer tokenizer, DataFrame rawData, String... param) {

        // TODO: 2016/6/12  ���ش�worddata
        DataFrame wordsData = tokenizer.transform(rawData);

        return wordsData;
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
