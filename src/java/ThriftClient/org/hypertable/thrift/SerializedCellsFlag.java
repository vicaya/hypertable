/**
 * Copyright (C) 2010  Doug Judd (Hypertable, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.thrift;

public class SerializedCellsFlag {
  public static final byte EOB            = (byte)0x01;
  public static final byte EOS            = (byte)0x02;
  public static final byte FLUSH          = (byte)0x04;
  public static final byte REV_IS_TS      = (byte)0x10;
  public static final byte AUTO_TIMESTAMP = (byte)0x20;
  public static final byte HAVE_TIMESTAMP = (byte)0x40;
  public static final byte HAVE_REVISION  = (byte)0x80;

  public static final long NULL = Long.MIN_VALUE + 1;
  public static final long AUTO_ASSIGN = Long.MIN_VALUE + 2;
}
