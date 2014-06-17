/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.OutputFormat;

import org.apache.thrift.TException;

import org.hypertable.thrift.ThriftClient;
import org.hypertable.thriftgen.*;

/**
 * Trivial reducer which simply writes the values(CellWritable) into Hypertable
 *
 *
 */
public abstract class IdentityReducer
extends Reducer<KeyWritable, BytesWritable> {
  private static final Log log = LogFactory.getLog(IdentityReducer.class);

  @Override
  public void reduce(KeyWritable key, Iterable<BytesWritable> values, Context context)
      throws IOException, InterruptedException {
    for(BytesWritable cell : values) {
      context.write(key, cell);
    }
  }
}

