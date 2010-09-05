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

package org.hypertable.hadoop.mapred;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Progressable;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConfigurable;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsWriter;


/**
 * Write Map/Reduce output to a table in Hypertable.
 *
 * TODO: For now we assume ThriftBroker is running on localhost on default port (38080).
 * Change this to read from configs at some point.
 */
public class TextTableOutputFormat implements org.apache.hadoop.mapred.OutputFormat<Text, Text> {
  private static final Log log = LogFactory.getLog(TextTableOutputFormat.class);

  public static final String NAMESPACE = "hypertable.mapreduce.output.namespace";
  public static final String TABLE = "hypertable.mapreduce.output.table";
  public static final String MUTATOR_FLAGS = "hypertable.mapreduce.output.mutator-flags";
  public static final String MUTATOR_FLUSH_INTERVAL = "hypertable.mapreduce.output.mutator-flush-interval";
  public static final int CLIENT_BUFFER_SIZE = 1024*1024*12;

  /**
   * Write reducer output to HT via Thrift interface
   *
   */
  protected static class HypertableRecordWriter implements RecordWriter<Text, Text> {
    private ThriftClient mClient;
    private long mNamespaceId=0;
    private long mMutator;
    private String namespace;
    private String table;

    private Text m_line = new Text();

    private SerializedCellsWriter mCellsWriter = new SerializedCellsWriter(CLIENT_BUFFER_SIZE);

    private static String utf8 = "UTF-8";
    private static final byte[] tab;
    private static final byte[] colon;
    private static final String colon_str;
    private static final String tab_str;
    static {
      try {
        tab = "\t".getBytes(utf8);
        tab_str = new String(tab);
        colon = ":".getBytes(utf8);
        colon_str = new String(colon);
      } catch (UnsupportedEncodingException uee) {
        throw new IllegalArgumentException("can't find " + utf8 + " encoding");
      }
   }


    /**
     * Opens a client & mutator to specified table
     *
     * @param namespace namespace containing HT table
     * @param table name of HT table
     * @param flags mutator flags
     * @param flush_interval used for periodic flush mutators
     */
    public HypertableRecordWriter(String namespace, String table, int flags, int flush_interval)
      throws IOException {
      try {
        //TODO: read this from HT configs
        this.namespace = namespace;
        this.table = table;
        mClient = ThriftClient.create("localhost", 38080);
        mNamespaceId = mClient.open_namespace(namespace);
        mMutator = mClient.open_mutator(mNamespaceId, table, flags, flush_interval);

      }
      catch (Exception e) {
        log.error(e);
        throw new IOException("Unable to open thrift mutator - " + e.toString());
      }
    }

    /**
     * Ctor with default flags=NO_LOG_SYNC and flush interval set to 0
     */
    public HypertableRecordWriter(String namespace, String table) throws IOException {
      this(namespace, table, MutatorFlag.NO_LOG_SYNC.getValue(), 0);
    }

    /**
     * Ctor with default flush interval set to 0
     */
    public HypertableRecordWriter(String namespace, String table, int flags) throws IOException {
      this(namespace, table, flags, 0);
    }

    /**
     * Close mutator and client
     * @param reporter
     */
    public void close(Reporter reporter) throws IOException {
      try {
        if (!mCellsWriter.isEmpty()) {
          mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), false);
          mCellsWriter.clear();
        }
        if (mNamespaceId != 0)
          mClient.close_namespace(mNamespaceId);
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
    public void write(Text key, Text value) throws IOException {
      try {
        key.append(tab, 0, tab.length);

        m_line.clear();
        m_line.append(key.getBytes(), 0, key.getLength());
        m_line.append(value.getBytes(), 0, value.getLength());
        int len = m_line.getLength();

        int tab_count=0;
        int tab_pos=0;
        int found=0;
        while(found != -1) {
          found = m_line.find(tab_str, found+1);
          if(found > 0) {
            tab_count++;
            if(tab_count == 1) {
              tab_pos = found;
            }
          }
        }

        boolean has_timestamp;
        if(tab_count >= 3) {
          has_timestamp = true;
        } else if (tab_count == 2) {
          has_timestamp = false;
        } else {
          throw new Exception("incorrect output line format only " + tab_count + " tabs");
        }

        byte [] byte_array = m_line.getBytes();
        int row_offset, row_length;
        int family_offset=0, family_length=0;
        int qualifier_offset=0, qualifier_length=0;
        int value_offset=0, value_length=0;
        long timestamp = SerializedCellsFlag.AUTO_ASSIGN;

        int offset=0;
        if(has_timestamp) {
          timestamp = Long.parseLong(m_line.decode(byte_array, 0, tab_pos));
          offset = tab_pos+1;
        }

        row_offset = offset;
        tab_pos = m_line.find(tab_str, offset);
        row_length = tab_pos - row_offset;

        offset = tab_pos+1;
        family_offset = offset;

        tab_pos = m_line.find(tab_str, offset);
        for (int i=family_offset; i<tab_pos; i++) {
          if (byte_array[i] == ':' && qualifier_offset == 0) {
            family_length = i-family_offset;
            qualifier_offset = i+1;
          }
        }
        // no qualifier
        if (qualifier_offset == 0)
          family_length = tab_pos-family_offset;
        else
          qualifier_length = tab_pos-qualifier_offset;

        offset = tab_pos+1;
        value_offset = offset;
        value_length = len-value_offset;

        if (!mCellsWriter.add(byte_array, row_offset, row_length,
                              byte_array, family_offset, family_length,
                              byte_array, qualifier_offset, qualifier_length,
                              timestamp,
                              byte_array, value_offset, value_length)) {
          mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), false);
          mCellsWriter.clear();
          if ((row_length+family_length+qualifier_length+value_length+32) > mCellsWriter.capacity())
            mCellsWriter = new SerializedCellsWriter(row_length+family_length+qualifier_length+value_length+32);
          if (!mCellsWriter.add(byte_array, row_offset, row_length,
                                byte_array, family_offset, family_length,
                                byte_array, qualifier_offset, qualifier_length,
                                timestamp,
                                byte_array, value_offset, value_length))
            throw new IOException("Unable to add cell to SerializedCellsWriter (row='" +
                                  new String(byte_array,row_offset,row_length,"UTF-8") + "'");
        }
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
  public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress)
    throws IOException {
    String namespace = job.get(TextTableOutputFormat.NAMESPACE);
    String table = job.get(TextTableOutputFormat.TABLE);
    int flags = job.getInt(TextTableOutputFormat.MUTATOR_FLAGS, 0);
    int flush_interval = job.getInt(TextTableOutputFormat.MUTATOR_FLUSH_INTERVAL, 0);

    try {
      return new HypertableRecordWriter(namespace, table, flags, flush_interval);
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
    public void checkOutputSpecs(FileSystem ignore, JobConf conf)
      throws IOException {
    try {
      //mClient.get_table_id();
    }
    catch (Exception e) {
      log.error(e);
      throw new IOException("Unable to get table id - " + e.toString());
    }
  }

}

