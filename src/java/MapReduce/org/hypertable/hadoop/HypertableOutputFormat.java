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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;


/**
 * Write Map/Reduce output to a table in Hypertable.
 *
 * TODO: For now we assume ThriftBroker is running on localhost on default port (38080).
 * Change this to read from configs at some point.
 * Key is not used but output value must be a KeyWritable
 */
public class HypertableOutputFormat extends OutputFormat<KeyWritable, BytesWritable> {
  private static final Log log = LogFactory.getLog(HypertableOutputFormat.class);

  /**
   * Write reducer output to HT via Thrift interface
   *
   */
  protected static class HypertableRecordWriter extends RecordWriter<KeyWritable, BytesWritable > {
    private ThriftClient mClient;
    private long mMutator;
    private String table;
    /**
     * Opens a client & mutator to specified table
     *
     * @param table name of HT table
     * @param flags mutator flags
     * @param flush_interval used for periodic flush mutators
     */
    public HypertableRecordWriter(String table, int flags, int flush_interval)
      throws IOException {
      try {
        //TODO: read this from HT configs
        this.table = table;
        mClient = ThriftClient.create("localhost", 38080);
        mMutator = mClient.open_mutator(table, flags, flush_interval);
      }
      catch (Exception e) {
        log.error(e);
        throw new IOException("Unable to open thrift mutator - " + e.toString());
      }
    }

    /**
     * Ctor with default flags=NO_LOG_SYNC and flush interval set to 0
     */
    public HypertableRecordWriter(String table) throws IOException {
      this(table, MutatorFlag.NO_LOG_SYNC.getValue(), 0);
    }

    /**
     * Ctor with default flush interval set to 0
     */
    public HypertableRecordWriter(String table, int flags) throws IOException {
      this(table, flags, 0);
    }

    /**
     * Close mutator and client
     * @param ctx
     */
    @Override
      public void close(TaskAttemptContext ctx) throws IOException {
      try {
        mClient.close_mutator(mMutator, true);
      }
      catch (Exception e) {
        log.error(e);
        throw new IOException("Unable to close thrift mutator - " + e.toString());
      }
    }

    /**
     * Write data to HT
     */
    @Override
      public void write(KeyWritable key, BytesWritable value) throws IOException {
      try {
        Cell cell = new Cell();
        cell.key = key;
        cell.value = value.getBytes();
        mClient.set_cell(mMutator, cell);
        log.info("Wrote cell with key " + key.row);
      }
      catch (Exception e) {
        log.error(e);
        throw new IOException("Unable to write cell - " + e.toString());
      }
    }
  }

  /**
   * Create a record writer
   */
  public RecordWriter<KeyWritable, BytesWritable> getRecordWriter(TaskAttemptContext ctx)
    throws IOException {
    String table = ctx.getConfiguration().get("HypertableOutputFormat.OUTPUT_TABLE");
    int flags = ctx.getConfiguration().getInt("HypertableOutputFormat.MUTATOR_FLAGS", 0);
    int flush_interval = ctx.getConfiguration().getInt("HypertableOutputFormat.MUTATOR_FLUSH_INTERVAL", 0);

    try {
      return new HypertableRecordWriter(table, flags, flush_interval);
    }
    catch (Exception e) {
      log.error(e);
      throw new IOException("Unable to access RecordWriter - " + e.toString());
    }
  }

  /**
   * TODO: Do something meaningful here
   * Make sure the table exists
   *
   */
  @Override
    public void checkOutputSpecs(JobContext ctx) throws IOException {
    try {
      //mClient.get_table_id();
    }
    catch (Exception e) {
      log.error(e);
      throw new IOException("Unable to get table id - " + e.toString());
    }
  }

  /**
   *
   *
   */
  @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext ctx) {
    return new HypertableOutputCommitter();
  }
}

