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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.io.BytesWritable;

import org.hypertable.thriftgen.ClientException;
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
  private String namespace;
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
    if (namespace == null) {
      namespace = job.get(HTSerDe.HT_NAMESPACE);
    }
    if (tablename == null) {
      tablename = job.get(HTSerDe.HT_TABLE_NAME);
    }

    String htColumnsMapping = job.get(HTSerDe.HT_COL_MAPPING);
    List<String> htColumnFamilies = new ArrayList<String>();
    List<String> htColumnQualifiers = new ArrayList<String>();
    List<byte []> htColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> htColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HTSerDe.parseColumnMapping(htColumnsMapping, htColumnFamilies,
          htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(job);

    if (htColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    boolean keys_only = true;
    scanspec.unsetColumns();
    if (!addAll) {
      for (int ii : readColIDs) {
        if (ii == iKey) {
          continue;
        }

        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
        keys_only = false;
      }
    } else {
      for (int ii=0; ii<htColumnFamilies.size(); ii++) {
        if (ii == iKey)
          continue;
        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
      }
      keys_only = false;
    }

    // The Hypertable table's row key maps to a Hive table column.
    // In the corner case when only the row key column is selected in Hive,
    // ask HT to return keys only
    if (keys_only)
      scanspec.setKeys_only(true);

    scanspec.setRevs(1);

    ScanSpec spec = htSplit.getSplit().createScanSpec(scanspec);

    htRowInputFormat.set_scan_spec(spec);
    htRowInputFormat.set_namespace(namespace);
    htRowInputFormat.set_table_name(tablename);

    return (RecordReader<K, V>)
      htRowInputFormat.getRecordReader(htSplit.getSplit(), job, reporter);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {

    Path [] tableNames = FileInputFormat.getInputPaths(job);
    String htNamespace = job.get(HTSerDe.HT_NAMESPACE);
    String htTableName = job.get(HTSerDe.HT_TABLE_NAME);
    htRowInputFormat.set_namespace(htNamespace);
    htRowInputFormat.set_table_name(htTableName);

    String htColumnsMapping = job.get(HTSerDe.HT_COL_MAPPING);
    if (htColumnsMapping == null) {
      throw new IOException("hypertable.columns.mapping required for Hypertable Table.");
    }

    List<String> htColumnFamilies = new ArrayList<String>();
    List<String> htColumnQualifiers = new ArrayList<String>();
    List<byte []> htColumnFamiliesBytes = new ArrayList<byte []>();
    List<byte []> htColumnQualifiersBytes = new ArrayList<byte []>();

    int iKey;
    try {
      iKey = HTSerDe.parseColumnMapping(htColumnsMapping, htColumnFamilies,
          htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);
    } catch (SerDeException se) {
      throw new IOException(se);
    }
    List<Integer> readColIDs = ColumnProjectionUtils.getReadColumnIDs(job);

    if (htColumnFamilies.size() < readColIDs.size()) {
      throw new IOException("Cannot read more columns than the given table contains.");
    }

    boolean addAll = (readColIDs.size() == 0);
    boolean keys_only = true;
    scanspec.unsetColumns();
    if (!addAll) {
      for (int ii : readColIDs) {
        if (ii == iKey) {
          continue;
        }

        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
        keys_only = false;
      }
    } else {
      for (int ii=0; ii<htColumnFamilies.size(); ii++) {
        if (ii == iKey)
          continue;
        if (htColumnQualifiers.get(ii) == null) {
          scanspec.addToColumns(htColumnFamilies.get(ii));
        } else {
          String column = htColumnFamilies.get(ii)+ ":" + htColumnQualifiers.get(ii);
          scanspec.addToColumns(column);
        }
      }
      keys_only = false;
    }
    // The Hypertable table's row key maps to a Hive table column.
    // In the corner case when only the row key column is selected in Hive,
    // ask HT to return keys only
    if (keys_only)
      scanspec.setKeys_only(true);
    scanspec.setRevs(1);

    htRowInputFormat.set_scan_spec(scanspec);

    int num_splits=0;
    InputSplit [] splits = htRowInputFormat.getSplits(job, num_splits);
    InputSplit [] results = new InputSplit[splits.length];
    for (int ii=0; ii< splits.length; ii++) {
      results[ii] = new HiveHTSplit((TableSplit) splits[ii], htColumnsMapping, tableNames[0]);
    }
    return results;
  }

  @Override
  public void configure(JobConf job) {
    htRowInputFormat.configure(job);
  }
}
