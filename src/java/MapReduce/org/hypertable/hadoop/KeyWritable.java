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

package org.hypertable.MapReduce.hadoop;

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

  public void readFields(DataInput in) throws IOException {
    row = WritableUtils.readString(in);
    column_family = WritableUtils.readString(in);
    column_qualifier = WritableUtils.readString(in);
    flag = in.readShort();
    timestamp = in.readLong();
    revision = in.readLong();
  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, row);
    WritableUtils.writeString(out, column_family);
    WritableUtils.writeString(out, column_qualifier);
    out.writeShort(flag);
    out.writeLong(timestamp);
    out.writeLong(revision);
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
}
