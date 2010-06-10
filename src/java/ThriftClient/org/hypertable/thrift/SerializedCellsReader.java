/**
 * Copyright (C) 2010  Doug Judd (Hypertable, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */


package org.hypertable.thrift;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class SerializedCellsReader {

  public SerializedCellsReader(byte [] buf) {
    mBase = buf;
    if (buf != null) {
      mBuf = ByteBuffer.wrap(buf);
      mBuf.order(ByteOrder.LITTLE_ENDIAN);
    }
  }

  public void reset(byte [] buf) {
    mBase = buf;
    mBuf = ByteBuffer.wrap(buf);
    mBuf.order(ByteOrder.LITTLE_ENDIAN);
    mEob = false;
    mRow = null;
    mColumnFamily = null;
    mColumnQualifier = null;
    mValue = null;
  }

  public boolean next() {
    int offset;
    long revision;

    if (mEob)
      return false;

    mFlag = mBuf.get();

    if ((mFlag & SerializedCellsFlag.EOB) != 0) {
      mEob = true;
      return false;
    }

    if ((mFlag & SerializedCellsFlag.HAVE_TIMESTAMP) != 0)
      mTimestamp = mBuf.getLong();

    if ((mFlag & SerializedCellsFlag.HAVE_REVISION) != 0 &&
        (mFlag & SerializedCellsFlag.REV_IS_TS) == 0)
      revision = mBuf.getLong();

    // row
    mRow = null;
    mRowOffset = mBuf.position();
    for (offset=mBuf.position(); mBuf.get(offset)!=0; offset++)
      ;
    mRowLength = offset - mRowOffset;
    mBuf.position(offset+1); // skip \0

    // column_family
    mColumnFamily = null;
    mColumnFamilyOffset = mBuf.position();
    for (offset=mBuf.position(); mBuf.get(offset)!=0; offset++)
      ;
    mColumnFamilyLength = offset - mColumnFamilyOffset;
    mBuf.position(offset+1); // skip \0

    // column_qualifier
    mColumnQualifier = null;
    mColumnQualifierOffset = mBuf.position();
    for (offset=mBuf.position(); mBuf.get(offset)!=0; offset++)
      ;
    mColumnQualifierLength = offset - mColumnQualifierOffset;
    mBuf.position(offset+1); // skip \0

    mValue = null;
    mValueLength = mBuf.getInt();
    mValueOffset = mBuf.position();
    if (mValueLength < 0) {
      System.out.println("vo=" + mValueOffset + ", vl=" + mValueLength + ", bl=" + mBuf.limit());
      System.out.println("ro=" + mRowOffset + ", rl=" + mRowLength);
      System.out.println("cfo=" + mColumnFamilyOffset + ", cfl=" + mColumnFamilyLength);
      System.out.println("cqo=" + mColumnQualifierOffset + ", cql=" + mColumnQualifierLength);
      System.exit(-1);
    }

    mBuf.position(mValueOffset+mValueLength);

    return true;
  }

  public byte [] get_row() {
    if (mRow == null) {
      mRow = new byte [ mRowLength ];
      System.arraycopy(mBase, mRowOffset, mRow, 0, mRowLength);
    }
    return mRow;
  }

  public int get_row_length() { return mRowLength; }

  public byte [] get_column_family() {
    if (mColumnFamily == null) {
      mColumnFamily = new byte [ mColumnFamilyLength ];
      System.arraycopy(mBase, mColumnFamilyOffset, mColumnFamily, 0, mColumnFamilyLength);
    }
    return mColumnFamily;
  }

  public int get_column_family_length() { return mColumnFamilyLength; }

  public byte [] get_column_qualifier() {
    if (mColumnQualifier == null) {
      mColumnQualifier = new byte [ mColumnQualifierLength ];
      System.arraycopy(mBase, mColumnQualifierOffset, mColumnQualifier, 0, mColumnQualifierLength);
    }
    return mColumnQualifier;
  }

  public int get_column_qualifier_length() { return mColumnQualifierLength; }

  public long get_timestamp() {
    return mTimestamp;
  }

  public byte [] get_value() {
    if (mValue == null) {
      mValue = new byte [ mValueLength ];
      System.arraycopy(mBase, mValueOffset, mValue, 0, mValueLength);
    }
    return mValue;
  }

  public int get_value_length() { return mValueLength; }

  public boolean eos() {
    return (mFlag & SerializedCellsFlag.EOS) != 0;
  }

  private boolean mEob = false;
  private byte [] mBase;
  private ByteBuffer mBuf;
  private byte mFlag;
  private byte [] mRow;
  private int mRowOffset;
  private int mRowLength;
  private byte [] mColumnFamily;
  private int mColumnFamilyOffset;
  private int mColumnFamilyLength;
  private byte [] mColumnQualifier;
  private int mColumnQualifierOffset;
  private int mColumnQualifierLength;
  private byte [] mValue;
  private int mValueOffset;
  private int mValueLength;
  private long mTimestamp;
}
