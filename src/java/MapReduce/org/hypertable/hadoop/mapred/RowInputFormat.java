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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import org.hypertable.hadoop.mapred.TableSplit;

import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.thrift.SerializedCellsReader;
import org.hypertable.hadoop.util.Row;
import org.hypertable.hadoop.mapreduce.ScanSpec;

import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

public class RowInputFormat
implements org.apache.hadoop.mapred.InputFormat<BytesWritable, Row>, JobConfigurable {

  final Log LOG = LogFactory.getLog(InputFormat.class);

  public static final String TABLE = "hypertable.mapreduce.input.table";
  public static final String SCAN_SPEC = "hypertable.mapreduce.input.scan-spec";
  public static final String START_ROW = "hypertable.mapreduce.input.startrow";
  public static final String END_ROW = "hypertable.mapreduce.input.endrow";

  private ThriftClient m_client = null;
  private ScanSpec m_base_spec = null;
  private String m_tablename = null;

  public void configure(JobConf job)
  {
    try {
      if (m_base_spec == null) {
        if(job.get(SCAN_SPEC) == null) {
          job.set(SCAN_SPEC, (new ScanSpec()).toSerializedText());
        }
        m_base_spec = ScanSpec.serializedTextToScanSpec( job.get(SCAN_SPEC) );
        m_base_spec.setRevs(1);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void set_scan_spec(ScanSpec spec) {
    m_base_spec = spec;
    m_base_spec.setRevs(1);
  }

  public void set_table_name(String tablename) {
    m_tablename = tablename;
  }
  protected class HypertableRecordReader
  implements org.apache.hadoop.mapred.RecordReader<BytesWritable, Row> {

    private ThriftClient m_client = null;
    private long m_scanner = 0;
    private String m_tablename = null;
    private ScanSpec m_scan_spec = null;
    private long m_bytes_read = 0;

    private byte m_serialized_cells[] = null;
    private Row m_value;
    private byte m_row[] = null;
    private BytesWritable m_key = null;
    private SerializedCellsReader m_reader = new SerializedCellsReader(null);

    private boolean m_eos = false;

    /**
     *  Constructor
     *
     * @param client Hypertable Thrift client
     * @param tablename name of table to read from
     * @param scan_spec scan specification
     */
    public HypertableRecordReader(ThriftClient client, String tablename, ScanSpec scan_spec)
     throws IOException {
      m_client = client;
      m_tablename = tablename;
      m_scan_spec = scan_spec;
      try {
        m_scanner = m_client.open_scanner(m_tablename, m_scan_spec, true);
      }
      catch (TTransportException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }
      catch (TException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }
      catch (ClientException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }

    }

    public BytesWritable createKey() {
        return new BytesWritable();
    }

    public Row createValue() {
        return new Row();
    }

    public void close() {
      try {
        m_client.close_scanner(m_scanner);
      }
      catch (Exception e) {
        e.printStackTrace();
      }
    }

    public long getPos() throws IOException {
            return m_bytes_read;
    }
    public float getProgress() {
      // Assume 200M split size
      if (m_bytes_read >= 200000000)
        return (float)1.0;
      return (float)m_bytes_read / (float)200000000.0;
    }

  public boolean next(BytesWritable key, Row value) throws IOException {
      try {
        if (m_eos)
          return false;

        m_row = m_client.next_row_serialized(m_scanner);
        m_reader.reset(m_row);
        if (m_reader.next()) {
          m_key = new BytesWritable(m_reader.get_row());
          m_value = new Row(m_row);
        }
        else {
          if (m_reader.eos()) {
            m_eos = true;
            return false;
          }
        }
        key.set(m_key);
        value.set(m_value);
        m_bytes_read += value.getSerializedRow().length;
      }
      catch (TTransportException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }
      catch (TException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }
      catch (ClientException e) {
        e.printStackTrace();
        throw new IOException(e.getMessage());
      }
      return true;
    }

  }


  public RecordReader<BytesWritable, Row> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
  throws IOException {
    try {
      TableSplit ts = (TableSplit)split;
      if (m_tablename == null) {
        m_tablename = job.get(TABLE);
      }
      ScanSpec scan_spec = ts.createScanSpec(m_base_spec);

      if (m_client == null)
        m_client = ThriftClient.create("localhost", 38080);
      return new HypertableRecordReader(m_client, m_tablename, scan_spec);
    }
    catch (TTransportException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (TException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    try {
      if (m_client == null)
        m_client = ThriftClient.create("localhost", 38080);

      String tablename;
      if (m_tablename == null)
        tablename = job.get(TABLE);
      else
        tablename = m_tablename;

      List<org.hypertable.thriftgen.TableSplit> tsplits = m_client.get_table_splits(tablename);
      InputSplit [] splits = new InputSplit[tsplits.size()];

      int pos=0;
      for (final org.hypertable.thriftgen.TableSplit ts : tsplits) {
        byte [] start_row = (ts.start_row == null) ? null : ts.start_row.getBytes();
        byte [] end_row = (ts.end_row == null) ? null : ts.end_row.getBytes();

        TableSplit split = new TableSplit(tablename.getBytes(), start_row, end_row,
                                          ts.ip_address);
        splits[pos++] = (InputSplit)split;
      }

      return splits;
    }
    catch (TTransportException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (TException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
    catch (ClientException e) {
      e.printStackTrace();
      throw new IOException(e.getMessage());
    }
  }
}


