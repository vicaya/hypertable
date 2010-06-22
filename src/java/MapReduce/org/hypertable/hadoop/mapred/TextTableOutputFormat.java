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


/**
 * Write Map/Reduce output to a table in Hypertable.
 *
 * TODO: For now we assume ThriftBroker is running on localhost on default port (38080).
 * Change this to read from configs at some point.
 */
public class TextTableOutputFormat implements org.apache.hadoop.mapred.OutputFormat<Text, Text> {
  private static final Log log = LogFactory.getLog(TextTableOutputFormat.class);

  public static final String TABLE = "hypertable.mapreduce.output.table";
  public static final String MUTATOR_FLAGS = "hypertable.mapreduce.output.mutator-flags";
  public static final String MUTATOR_FLUSH_INTERVAL = "hypertable.mapreduce.output.mutator-flush-interval";


  /**
   * Write reducer output to HT via Thrift interface
   *
   */
  protected static class HypertableRecordWriter implements RecordWriter<Text, Text> {
    private ThriftClient mClient;
    private long mMutator;
    private String table;

    private Text m_line = new Text();

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
     * @param reporter
     */
    public void close(Reporter reporter) throws IOException {
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

        Cell cell = new Cell();
        Key t_key = new Key();

        int offset=0;
        if(has_timestamp) {
          t_key.timestamp = Long.parseLong(m_line.decode(m_line.getBytes(), 0, tab_pos));
          offset = tab_pos+1;
        }

        tab_pos = m_line.find(tab_str, offset);
        t_key.row = m_line.decode(m_line.getBytes(),offset,tab_pos-offset);
        offset = tab_pos+1;
        

        tab_pos = m_line.find(tab_str, offset);
        String cols[] = m_line.decode(m_line.getBytes(),offset,tab_pos-offset).split(colon_str);
        t_key.column_family = cols[0];
        t_key.column_qualifier = cols[1];
        offset = tab_pos+1;

        cell.key = t_key;
        cell.value = m_line.decode(m_line.getBytes(),offset,len-offset).getBytes();

        mClient.set_cell(mMutator, cell);

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

    String table = job.get(TextTableOutputFormat.TABLE);
    int flags = job.getInt(TextTableOutputFormat.MUTATOR_FLAGS, 0);
    int flush_interval = job.getInt(TextTableOutputFormat.MUTATOR_FLUSH_INTERVAL, 0);

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

