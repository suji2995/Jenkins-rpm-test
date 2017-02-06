package com.rackspace.volga.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/* Alarm state processor
*
* Copyright 2014 Rackspace Hosting, Inc.
*/


class AlarmStateProcessor {

    private static final Log LOG = LogFactory.getLog(AlarmStateProcessor.class.getName());

    private List<LongState> data;

    private class LongState implements Comparable<LongState> {
        Long ts;

        Long state;

        LongState(Long ts, long state) {
            this.ts = ts;
            this.state = state;
        }

        @Override
        public int compareTo(LongState other) {
            return this.ts.compareTo(other.ts);
        }
    }


    public AlarmStateProcessor() {
        this.data = new ArrayList<LongState>();
    }

    // Add a timestamp, state string
    void add(Long ts, String state_s) {
        AlarmState state = AlarmState.makeState(state_s);

        data.add(new LongState(ts, state.asLong()));
    }

    // Serialize the object to a Hadoop-serializer friendly object
    public ArrayList<LongWritable> serialize() {
        ArrayList<LongWritable> result = new ArrayList<LongWritable>();
        int size = data.size();
        for (int i = 0; i < size; i++) {
            result.add(new LongWritable(data.get(i).ts));
            result.add(new LongWritable(data.get(i).state));
        }
        return result;
    }

    // Initialize the object from a Hadoop-serializer friendly object
    public void deserialize(ArrayList<LongWritable> obj) {
        this.data = new ArrayList<LongState>();

        int size = obj.size();
        for (int i = 0; i < size; i += 2) {
            Long ts = obj.get(i).get();
            Long state = obj.get(i + 1).get();
            this.data.add(new LongState(ts, state));
        }
    }

    // merge contents of another ASP object into this
    public void merge(List<LongWritable> other) {
        int size = other.size();
        for (int i = 0; i < size; i += 2) {
            Long ts = other.get(i).get();
            Long state_i = other.get(i + 1).get();

            this.data.add(new LongState(ts, state_i));
        }
    }

    // Return the type returned by @computeResult():
    //   a list of struct (2-fields) of LongWritable
    public static ObjectInspector getResultOI() {
        ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
        ArrayList<String> fname = new ArrayList<String>();
        fname.add("opened");
        fname.add("closed");

        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory
                .getStandardStructObjectInspector(fname, foi));
    }

    private Object makeOpenClosed(Long opened_ts, Long closed_ts) {
        List<LongWritable> result = new ArrayList<LongWritable>(2);
        LongWritable opened = null;
        LongWritable closed = null;

        if (opened_ts != null) {
            opened = new LongWritable(opened_ts);
        }
        if (closed_ts != null) {
            closed = new LongWritable(closed_ts);
        }

        result.add(opened);
        result.add(closed);

        return result;
    }


    // compute the final ASP results: struct of two LongWritable (may be null)
    public Object computeResult() {
        Long opened_ts = null;
        Long closed_ts = null;

        int size = data.size();

        Collections.sort(data);

        List<Object> result = new ArrayList<Object>();

        AlarmState last_state = AlarmState.UNKNOWN;
        Long last_ts = null;
        for (int i = 0; i < size; i++) {
            LongState ls = data.get(i);
            Long ts = ls.ts;
            AlarmState current_state = AlarmState.makeState(ls.state);

            if (last_state == AlarmState.WARNING &&
                    current_state == AlarmState.OK) {
                // ignore - not interested
                LOG.debug("Ignoring warning to ok");
            } else if (last_state == AlarmState.OK &&
                    current_state == AlarmState.WARNING) {
                // ignore - not interested
                LOG.debug("Ignoring ok to warning");
            } else if (last_state == AlarmState.CRITICAL) {
                // set opened TS. leave open until critical is succeded
                // by warning or ok
                opened_ts = last_ts;
                closed_ts = null;
                LOG.debug("Start alarm critical at " + last_ts);

                if (current_state == AlarmState.OK) {
                    // set closed TS
                    closed_ts = ts;
                    result.add(makeOpenClosed(opened_ts, closed_ts));
                    LOG.debug("Stop alarm critical (ok) at " + ts);
                } else if (current_state == AlarmState.WARNING) {
                    // set closed TS to warning time; warning is considered
                    // closed
                    closed_ts = ts;
                    result.add(makeOpenClosed(opened_ts, closed_ts));
                    LOG.debug("Stop alarm critical (warning) at " + ts);
                }
            }

            last_state = current_state;
            last_ts = ts;

            // If this is the last record then record alarm as open
            if (i == (size - 1)) {
                if (last_state == AlarmState.CRITICAL) {
                    opened_ts = last_ts;
                    closed_ts = null;
                    result.add(makeOpenClosed(opened_ts, closed_ts));
                    LOG.debug("Alarm ended timeslot in state critical at " + last_ts);
                }
            }
        }

        return result;
    }
}
