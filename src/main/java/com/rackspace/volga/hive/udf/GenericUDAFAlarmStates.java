package com.rackspace.volga.hive.udf;
/**
 * Copyright 2014 Rackspace Hosting, Inc.
 */

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

import java.util.List;


@Description(name = "alarmstates",
        value = "_FUNC_(ts, alarm state) - Aggregate function over group of ts, alarm state returning a struct",
        extended = "Example:\n"
                + "> SELECT alarm_id, alarmstates(ts, state) FROM notifications GROUP BY alarm_id;\n"
                + "")
public class GenericUDAFAlarmStates extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFAlarmStates.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        ObjectInspector[] ois = info.getParameterObjectInspectors();

        if (ois.length != 2) {
            throw new UDFArgumentTypeException(ois.length - 1,
                    "Please specify exactly two arguments.");
        }

        return new GenericUDAFAlarmStatesEvaluator();
    }

    public static class GenericUDAFAlarmStatesEvaluator extends GenericUDAFEvaluator {
        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector tsOI;

        private PrimitiveObjectInspector stateOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list of Longs)
        private StandardListObjectInspector loi;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);


            // init input object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                // original data parameters (always 2 parameters)
                assert (parameters.length == 2);
                tsOI = (PrimitiveObjectInspector) parameters[0];
                stateOI = (PrimitiveObjectInspector) parameters[1];
            } else {
                // partial aggregation object (always 1 parameter)
                assert (parameters.length == 1);
                loi = (StandardListObjectInspector) parameters[0];
            }

            // init output object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // The output of a partial aggregation is a list of Long
                // representing pairs of (timestamp, state). The list
                // length should *always* be even.
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        PrimitiveObjectInspectorFactory.writableLongObjectInspector);
            } else {
                // The output of FINAL and COMPLETE is a full aggregation
                return AlarmStateProcessor.getResultOI();
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            // return a single ArrayList from pairs of (Long ts, Long state)
            AlarmAgg alarmagg = (AlarmAgg) agg;
            return alarmagg.processor.serialize();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AlarmAgg alarmagg = (AlarmAgg) agg;
            // Return the final result of the aggregation to Hive
            return alarmagg.processor.computeResult();
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            // Merge a partial aggregation returned by terminatePartial
            // into the current aggregation
            if (partial == null) {
                return;
            }

            List<LongWritable> other = (List<LongWritable>) loi.getList(partial);
            AlarmAgg alarmagg = (AlarmAgg) agg;
            alarmagg.processor.merge(other);
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            // Process a new row of data into the aggregation buffer
            assert (parameters.length == 2);
            if (parameters[0] == null || parameters[1] == null) {
                return;
            }

            Long ts = PrimitiveObjectInspectorUtils.getLong(parameters[0], tsOI);
            String state_s = PrimitiveObjectInspectorUtils.getString(parameters[1], stateOI);
            AlarmAgg alarmagg = (AlarmAgg) agg;
            alarmagg.processor.add(ts, state_s);
        }

        // Aggregation buffer definition and manipulation methods
        static class AlarmAgg implements AggregationBuffer {
            AlarmStateProcessor processor;
        }

        ;

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AlarmAgg result = new AlarmAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AlarmAgg result = (AlarmAgg) agg;
            result.processor = new AlarmStateProcessor();
        }
    }
}
