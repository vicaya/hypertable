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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import java.nio.BufferUnderflowException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.thrift.TException;

import org.hypertable.AsyncComm.Comm;
import org.hypertable.AsyncComm.ResponseCallback;
import org.hypertable.Common.Error;
import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;

/**
 * Write Map/Reduce output to a table in Hypertable.
 *
 * TODO: For now we assume ThriftBroker is running on localhost on default port (38080).
 * Change this to read from configs at some point.
 */
public class CellWritable extends Cell implements WritableComparable<Cell> {

  public CellWritable() {super(); value = new byte[0]; }

  public void readFields(DataInput in) throws IOException {
    row_key = WritableUtils.readString(in);
    column_family = WritableUtils.readString(in);
    column_qualifier = WritableUtils.readString(in);
    flag = in.readShort();
    timestamp = in.readLong();
    revision = in.readLong();

    int len = WritableUtils.readVInt(in);
    if(value.length < len)
      value = new byte[len];
    in.readFully(value, 0, len);

  }

  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, row_key);
    WritableUtils.writeString(out, column_family);
    WritableUtils.writeString(out, column_qualifier);
    out.writeShort(flag);
    out.writeLong(timestamp);
    out.writeLong(revision);
    int length = value.length;
    WritableUtils.writeVInt(out, value.length);
    out.write(value);
  }

  @Override
  public int hashCode() {
    int code = row_key.hashCode()
               ^ column_family.hashCode()
               ^ (int) flag ^ (int) timestamp ^ (int) revision;
    if (column_qualifier != null)
      code ^= column_qualifier.hashCode();
    if (value != null)
      code ^= value.hashCode();
    return code;
  }
}

