package com.rackspace.volga.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import org.apache.commons.codec.binary.Base64;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Copyright Rackspace Hosting, Inc.
 *
 * hive> ADD JAR /path/to/jar;
 * hive> CREATE TEMPORARY FUNCTION SHA1 AS 'com.rackspace.volga.hive.udf.SHA1';
 *
 */
@Description(
        name = "SHA1",
        value = "_FUNC_(string) - return the SHA1 hash of the input string",
        extended = "Example:\n" +
                "  > SELECT _FUNC_(string) FROM table n;\n" +
                "  9Cw/dEfgH23456ABC3456yuABCD="
)
public class SHA1 extends UDF {
  static final String ALGORITHM = "SHA1";
  static final int LINE_LENGTH = 76;
  static final byte[] LINE_SEPARATOR = {};

  public Text evaluate(Text s) {
    if (s == null) {
      return null;
    }

    try {
      MessageDigest md = MessageDigest.getInstance(ALGORITHM);
      md.update(s.toString().getBytes());
      byte[] hash = md.digest();
      Base64 encoder = new Base64(LINE_LENGTH, LINE_SEPARATOR, true);

      return new Text(encoder.encodeToString(hash));
      } catch (NoSuchAlgorithmException nsae) {
      throw new IllegalArgumentException("Digest " + ALGORITHM + " is not available");
    }
  }
}
