/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

package org.hypertable.hadoop.mapreduce;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;

/**
 * Write Map/Reduce output to a table in Hypertable.
 *
 * TODO: For now we assume ThriftBroker is running on localhost on default port (38080).
 * Change this to read from configs at some point.
 */
public class KeyWritable extends Key implements WritableComparable<Key> {

  public KeyWritable() { super(); }

  @Override
  public void readFields(DataInput in) throws IOException {
    boolean isset;
    row = WritableUtils.readString(in);
    column_family = WritableUtils.readString(in);
    column_qualifier = WritableUtils.readString(in);
    flag = in.readShort();
    /** timestamp **/
    isset = in.readBoolean();
    setTimestampIsSet(isset);
    if (isset)
      timestamp = in.readLong();
    else
      timestamp = Long.MIN_VALUE+2;
    /** revision **/
    isset = in.readBoolean();
    setRevisionIsSet(isset);
    if (isset)
      revision = in.readLong();
    else
      revision = Long.MIN_VALUE+2;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, row);
    WritableUtils.writeString(out, column_family);
    WritableUtils.writeString(out, column_qualifier);
    out.writeShort(flag);
    if (isSetTimestamp()) {
      out.writeBoolean(true);
      out.writeLong(timestamp);
    }
    if (isSetRevision()) {
      out.writeBoolean(true);
      out.writeLong(revision);
    }
  }

  @Override
    public int hashCode() {
    int code = row.hashCode()
      ^ column_family.hashCode()
      ^ (int) flag ^ (int) timestamp ^ (int) revision;
    if (column_qualifier != null)
      code ^= column_qualifier.hashCode();
    return code;
  }

  public void load(Key key) { 
    if (key.isSetRow()) {
      row = key.row;
      setRowIsSet(true);
    }
    if (key.isSetColumn_family()) {
      column_family = key.column_family;
      setColumn_familyIsSet(true);
    }
    if (key.isSetColumn_qualifier()) {
      column_qualifier = key.column_qualifier;
      setColumn_qualifierIsSet(true);
    }
    if (key.isSetFlag()) {
      flag = key.flag;
      setFlagIsSet(true);
    }
    if (key.isSetTimestamp()) {
      timestamp = key.timestamp;
      setTimestampIsSet(true);
    }
    else
      timestamp = Long.MIN_VALUE + 2;
    if (key.isSetRevision()) {
      revision = key.revision;
      setRevisionIsSet(true);
    }
    else
      revision = Long.MIN_VALUE + 2;
  }


}
