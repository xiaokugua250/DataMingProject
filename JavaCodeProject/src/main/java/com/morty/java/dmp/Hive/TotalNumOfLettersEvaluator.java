package com.morty.java.dmp.Hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

/**
 * Created by morty on 2016/06/29.
 * // Object inspectors for input and output parameters
 * public  ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException;
 * <p>
 * // class to store the result of the data processing
 * abstract AggregationBuffer getNewAggregationBuffer() throws HiveException;
 * <p>
 * // reset Aggregation buffer
 * public void reset(AggregationBuffer agg) throws HiveException;
 * <p>
 * // process input record
 * public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException;
 * <p>
 * // finilize processing of a part of all the input data
 * public Object terminatePartial(AggregationBuffer agg) throws HiveException;
 * <p>
 * // add the results of two partial aggregations together
 * public void merge(AggregationBuffer agg, Object partial) throws HiveException;
 * <p>
 * // output final result
 * public Object terminate(AggregationBuffer agg) throws HiveException;
 */
public class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {
    int total = 0;
    PrimitiveObjectInspector inputOI;
    ObjectInspector outputOI;
    PrimitiveObjectInspector IntegerOI;
    private boolean warned = false;

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        assert (parameters.length == 1);
        super.init(m, parameters);

        if ((m == Mode.PARTIAL1) || (m == Mode.COMPLETE)) {
            inputOI = (PrimitiveObjectInspector) parameters[0];
        } else {
            IntegerOI = (PrimitiveObjectInspector) parameters[0];
        }

        outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
                ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

        return outputOI;
    }

    @Override
    public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
        assert (objects.length == 1);

        if (objects[0] != null) {
            LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;
            Object p1 = ((PrimitiveObjectInspector) inputOI.getPrimitiveJavaObject(objects[0]));

            myagg.add(String.valueOf(p1).length());
        }
    }

    @Override
    public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
        if (o != null) {
            LetterSumAgg myagg1 = (LetterSumAgg) aggregationBuffer;
            Integer partialSum = (Integer) IntegerOI.getPrimitiveJavaObject(o);
            LetterSumAgg myagg2 = new LetterSumAgg();

            myagg2.add(partialSum);
            myagg1.add(myagg2.sum);
        }
    }

    @Override
    public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
        LetterSumAgg myAgg = new LetterSumAgg();
    }

    @Override
    public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
        LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;

        total = myagg.sum;

        return myagg.sum;
    }

    @Override
    public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
        LetterSumAgg myagg = (LetterSumAgg) aggregationBuffer;

        total += myagg.sum;

        return total;
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
        LetterSumAgg result = new LetterSumAgg();

        return result;
    }

    static class LetterSumAgg implements AggregationBuffer {
        int sum = 0;

        void add(int num) {
            sum += num;
        }
    }
}


//~ Formatted by Jindent --- http://www.jindent.com
