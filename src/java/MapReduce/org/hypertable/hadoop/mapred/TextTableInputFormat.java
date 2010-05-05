/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;

import org.hypertable.thriftgen.*;
import org.hypertable.hadoop.mapreduce.ScanSpec;
import org.hypertable.hadoop.mapred.TextTableSplit;

import org.hypertable.thrift.ThriftClient;

import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

public class TextTableInputFormat
implements org.apache.hadoop.mapred.InputFormat<Text, Text>, JobConfigurable {
  
  final Log LOG = LogFactory.getLog(InputFormat.class);

  public static final String TABLE = "hypertable.mapreduce.input.table";
  public static final String SCAN_SPEC = "hypertable.mapreduce.input.scan-spec";
  public static final String START_ROW = "hypertable.mapreduce.input.startrow";
  public static final String END_ROW = "hypertable.mapreduce.input.endrow";
  public static final String HAS_TIMESTAMP = "hypertable.mapreduce.input.timestamp";

  private ThriftClient m_client = null;
  private ScanSpec m_base_spec = null;
  private String m_tablename = null;
  private boolean m_timestamp = false;

  public void configure(JobConf job)
  {
    m_timestamp = job.getBoolean(HAS_TIMESTAMP, false);
    try {
      if(job.get(SCAN_SPEC) == null) {
        job.set(SCAN_SPEC, (new ScanSpec()).toSerializedText());
      }
      m_base_spec = ScanSpec.serializedTextToScanSpec( job.get(SCAN_SPEC) );

      String start_row = job.get(START_ROW);
      String end_row = job.get(END_ROW);

      if(start_row != null || end_row != null) {
        RowInterval interval = new RowInterval();

        m_base_spec.unsetRow_intervals();

        if(start_row != null) {
          interval.setStart_row(start_row);
          interval.setStart_rowIsSet(true);
          interval.setStart_inclusive(false);
          interval.setStart_inclusiveIsSet(true);
        }

        if(end_row != null) {
          interval.setEnd_row(end_row);
          interval.setEnd_rowIsSet(true);
          interval.setEnd_inclusive(true);
          interval.setEnd_inclusiveIsSet(true);
        }

        if(interval.isSetStart_row() || interval.isSetEnd_row()) {
          m_base_spec.addToRow_intervals(interval);
          m_base_spec.setRow_intervalsIsSet(true);
        }
      }

      System.out.println(m_base_spec);
    }
    catch (Exception e) {
      e.printStackTrace();
    }

  }

  protected class HypertableRecordReader
  implements org.apache.hadoop.mapred.RecordReader<Text, Text> {

    private ThriftClient m_client = null;
    private long m_scanner = 0;
    private String m_tablename = null;
    private ScanSpec m_scan_spec = null;
    private long m_bytes_read = 0;

    private List<Cell> m_cells = null;
    private Iterator<Cell> m_iter = null;
    private boolean m_eos = false;

    private Text m_key = new Text();
    private Text m_value = new Text();

    private byte[] t_row = null;
    private byte[] t_column_family = null;
    private byte[] t_column_qualifier = null;
    private byte[] t_timestamp = null;

    /* XXX make this less ugly and actually use stream.seperator */
    private byte[] tab = "\t".getBytes();
    private byte[] colon = ":".getBytes();

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

    public Text createKey() {
        return new Text("");
    }

    public Text createValue() {
        return new Text("");
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

    private void fill_key(Text key, Key cell_key) { 
      boolean clear = false;
      /* XXX not sure if "clear" is necessary */

      if (m_timestamp && cell_key.isSetTimestamp()) {
        t_timestamp = Long.toString(cell_key.timestamp).getBytes();
        clear = true;
      }

      if (cell_key.isSetRow()) {
        t_row = cell_key.row.getBytes();
        clear = true;
      }
      if (cell_key.isSetColumn_family()) {
        t_column_family = cell_key.column_family.getBytes();
        clear = true;
      }
      if (cell_key.isSetColumn_qualifier()) {
        t_column_qualifier = cell_key.column_qualifier.getBytes();
        clear = true;
      }
      
      if(clear) {
          key.clear();
          if(m_timestamp) {
              key.append(t_timestamp,0,t_timestamp.length);
          }
          key.append(t_row,0,t_row.length);
          key.append(tab,0,tab.length);
          key.append(t_column_family,0,t_column_family.length);
          key.append(colon,0,colon.length);
          key.append(t_column_qualifier,0,t_column_qualifier.length);
      }
    }

  public boolean next(Text key, Text value) throws IOException {
      try {
        if (m_eos)
          return false;
        if (m_cells == null || !m_iter.hasNext()) {
          m_cells = m_client.next_cells(m_scanner);
          if (m_cells.isEmpty()) {
            m_eos = true;
            return false;
          }
          m_iter = m_cells.iterator();
        }
        Cell cell = m_iter.next();
        fill_key(key, cell.key);
        value.set(cell.value);
        m_bytes_read += 24 + cell.key.row.length() + cell.value.length;
        if (cell.key.column_qualifier != null)
          m_bytes_read += cell.key.column_qualifier.length();
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


  public RecordReader<Text, Text> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter)
  throws IOException {
    try {
      TextTableSplit ts = (TextTableSplit)split;
      if (m_tablename == null) {
        m_tablename = job.get(TABLE);
      }
      ScanSpec scan_spec = ts.createScanSpec(m_base_spec);
      System.out.println(scan_spec);

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

      String tablename = job.get(TABLE);

      List<org.hypertable.thriftgen.TableSplit> tsplits = m_client.get_table_splits(tablename);
      List<InputSplit> splits = new ArrayList<InputSplit>(tsplits.size());
      for (final org.hypertable.thriftgen.TableSplit ts : tsplits) {
        boolean skip = false;

        for(RowInterval ri: m_base_spec.getRow_intervals()) {
                if(ri.isSetStart_row() && ts.start_row != null) {
                    if (ri.getStart_row().compareTo(ts.start_row) >= 0) {
                        skip = true;
                    } 
                }
                if(ri.isSetEnd_row() && ts.end_row != null) {
                    if(ri.getEnd_row().compareTo(ts.end_row) < 0) {
                        skip = true;
                    }
                }
                if(ri.isSetStart_row() && ts.start_row == null && ts.end_row != null) {
                    if(ri.getStart_row().compareTo(ts.end_row) >= 0) {
                        skip = true;
                    }
                }
                if(ri.isSetEnd_row() && ts.end_row == null && ts.start_row != null) {
                    if(ri.getEnd_row().compareTo(ts.start_row) < 0) {
                        skip = true;
                    }
                }
            if(skip) {
                break;
            }
        }
        if(skip) { 
            continue;
        }
        byte [] start_row = (ts.start_row == null) ? null : ts.start_row.getBytes();
        byte [] end_row = (ts.end_row == null) ? null : ts.end_row.getBytes();

        TextTableSplit split = new TextTableSplit(tablename.getBytes(), start_row, end_row, ts.ip_address);
        splits.add(split);      
      }
      
      InputSplit[] isplits = new InputSplit[splits.size()];
      return splits.toArray(isplits);
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
