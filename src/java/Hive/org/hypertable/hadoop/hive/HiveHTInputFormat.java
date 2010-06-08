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

package org.hypertable.hadoop.hive;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.BytesWritable;

import org.hypertable.thriftgen.ClientException;
import org.hypertable.thrift.ThriftClient;
import org.hypertable.hadoop.util.Row;
import org.hypertable.hadoop.mapred.RowInputFormat;
import org.hypertable.hadoop.mapreduce.ScanSpec;
import org.hypertable.hadoop.mapred.TableSplit;

/**
 * HiveHTInputFormat implements InputFormat for Hypertable storage handler
 * tables, decorating an underlying Hypertable RowInputFormat with extra Hive logic
 * such as column pruning.
 */
public class HiveHTInputFormat<K extends BytesWritable, V extends Row>
    implements InputFormat<K, V>, JobConfigurable {

  static final Log LOG = LogFactory.getLog(HiveHTInputFormat.class);

  private RowInputFormat htRowInputFormat;
  private String tablename;
  private ScanSpec scanspec;

  public HiveHTInputFormat() {
    htRowInputFormat = new RowInputFormat();
    scanspec = new ScanSpec();
  }

  @Override
  public RecordReader<K, V> getRecordReader(
    InputSplit split, JobConf job,
    Reporter reporter) throws IOException {

   HiveHTSplit htSplit = (HiveHTSplit)split;
   if (tablename == null) {
     tablename = job.get(HTSerDe.HT_TABLE_NAME);
   }

   // because the hypertable key is mapped to the first column in its hive table,
   // we add the "_key" before the columnMapping that we can use the
   // hive column id to find the exact hypertable column one-for-one.
   String columnMapping = "_key," + htSplit.getColumnsMapping();
   String[] columns = columnMapping.split(",");
   List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(job);

   if (columns.length < readColIDs.size()) {
     throw new IOException(
       "Cannot read more columns than the given table contains.");
   }

   // add columns to scan spec
   scanspec.unsetColumns();
   if (readColIDs.size() > 0) {
     for (int ii=0; ii < columns.length - 1; ii++) {
       // currently HT doesn't support filtering by qualifier
       if (columns[ii+1].indexOf(':') != -1)
         columns[ii+1] = columns[ii+1].substring(0, columns[ii+1].indexOf(':'));

       scanspec.addToColumns(columns[ii+1]);
     }
   }

   scanspec.setRevs(1);

   ScanSpec spec = htSplit.getSplit().createScanSpec(scanspec);
   htRowInputFormat.set_scan_spec(spec);
   htRowInputFormat.set_table_name(tablename);
    return (RecordReader<K, V>)
      htRowInputFormat.getRecordReader(htSplit.getSplit(), job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    Path [] tableNames = FileInputFormat.getInputPaths(job);
    String htTableName = job.get(HTSerDe.HT_TABLE_NAME);
    htRowInputFormat.set_table_name(htTableName);

    String htSchemaMapping = job.get(HTSerDe.HT_COL_MAPPING);
    if (htSchemaMapping == null) {
      throw new IOException("hypertable.columns.mapping required for Hypertable Table.");
    }

    scanspec.unsetColumns();
    String [] columns = htSchemaMapping.split(",");
    for (int ii=0; ii < columns.length; ii++) {
      scanspec.addToColumns(columns[ii]);
    }
    scanspec.setRevs(1);
    htRowInputFormat.set_scan_spec(scanspec);

    int num_splits=0;
    InputSplit [] splits = htRowInputFormat.getSplits(job, num_splits);
    InputSplit [] results = new InputSplit[splits.length];
    for (int ii=0; ii< splits.length; ii++) {
      results[ii] = new HiveHTSplit((TableSplit) splits[ii], htSchemaMapping, tableNames[0]);
    }

    return results;
  }

  @Override
  public void configure(JobConf job) {
    htRowInputFormat.configure(job);
  }
}
