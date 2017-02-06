package com.rackspace.volga.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * User: alex.silva
 * Date: 5/19/14
 * Time: 9:54 AM
 * Copyright Rackspace Hosting, Inc.
 */
@Description(
        name = "hash",
        value = "_FUNC_(str) - Converts a string to uppercase",
        extended = "Example:\n" +
                "  > SELECT hash(event_id) FROM notifications n;\n" +
                "  123456422"
)
public class HashUDF extends UDF {

    public Text evaluate(Text s) {
        Text retTxt = new Text("");
        if (s != null) {
            try {
                int hc = s.toString().hashCode();
                retTxt.set(String.valueOf((hc ^ (hc >>> 32))));
            } catch (Exception e) { // Should never happen
                retTxt = new Text(s);
            }
        }
        return retTxt;
    }
}