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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.SerDe;

import org.hypertable.hadoop.mapred.RowOutputFormat;
import org.hypertable.hadoop.mapred.RowInputFormat;
import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;

/**
 * StorageHandler provides a HiveStorageHandler implementation for
 * Hypertable.
 */
public class HTStorageHandler
  implements HiveStorageHandler, HiveMetaHook {

  private long mNamespaceId=0;
  private ThriftClient mClient=null;
  private Configuration mConf=null;

   private String getHTNamespace(Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).
    String namespace = tbl.getParameters().get(HTSerDe.HT_NAMESPACE);
    if (namespace == null) {
      namespace = tbl.getSd().getSerdeInfo().getParameters().get(
        HTSerDe.HT_NAMESPACE);
    }
    return namespace;
  }

  private String getHTTableName(Table tbl) {
    // Give preference to TBLPROPERTIES over SERDEPROPERTIES
    // (really we should only use TBLPROPERTIES, so this is just
    // for backwards compatibility with the original specs).

    String tableName = tbl.getParameters().get(HTSerDe.HT_TABLE_NAME);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
        HTSerDe.HT_TABLE_NAME);
    }
    if (tableName == null) {
      tableName = tbl.getTableName();
    }
    return tableName;
  }

  @Override
  public void preDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public void commitDropTable(
    Table tbl, boolean deleteData) throws MetaException {

    try {
      String tableName = getHTTableName(tbl);
      boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
      if (deleteData && !isExternal) {
        // TODO:drop table
        throw new MetaException("Drop table not yet supported via HTStorageHandler. " +
                                "Drop directly from Hypertable for now.");
      }
      // nothing to do since this is an external table
    } catch (Exception ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void preCreateTable(Table tbl) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);

    // We'd like to move this to HiveMetaStore for any non-native table, but
    // first we need to support storing NULL for location on a table
    if (tbl.getSd().getLocation() != null) {
      throw new MetaException("LOCATION may not be specified for Hypertable.");
    }

    try {
      String namespace = getHTNamespace(tbl);
      String tblName = getHTTableName(tbl);
      Map<String, String> serdeParam = tbl.getSd().getSerdeInfo().getParameters();
      String htColumnsMapping = serdeParam.get(HTSerDe.HT_COL_MAPPING);

      if (htColumnsMapping == null) {
        throw new MetaException("No hypertable.columns.mapping defined in Serde.");
      }

      List<String> htColumnFamilies = new ArrayList<String>();
      List<String> htColumnQualifiers = new ArrayList<String>();
      List<byte []> htColumnFamiliesBytes = new ArrayList<byte []>();
      List<byte []> htColumnQualifiersBytes = new ArrayList<byte []>();
      int iKey = HTSerDe.parseColumnMapping(htColumnsMapping, htColumnFamilies,
          htColumnFamiliesBytes, htColumnQualifiers, htColumnQualifiersBytes);

      Set<String> uniqueColumnFamilies = new HashSet<String>(htColumnFamilies);
      uniqueColumnFamilies.remove(htColumnFamilies.get(iKey));
      if (mClient == null) {
        //TODO: read values from configs
        mClient = ThriftClient.create("localhost", 38080);
        mNamespaceId = mClient.open_namespace(namespace);
      }
      // TODO: support managed tables
      if (!isExternal) {
        // Implement this later since we'd want to allow all CF and AG features exposed by HQL
        throw new MetaException("Hypertable Storage handler only supports external" +
                                " tables currently.");
      }
      if (!mClient.exists_table(mNamespaceId, tblName)) {
        throw new MetaException("Hypertable table " + tblName +
            " doesn't exist while the table is declared as an external table.");
      }

      // sanity check to make sure the table has the specified cfs
      Schema schema = mClient.get_schema(mNamespaceId, tblName);
      for (int ii = 0; ii < htColumnFamilies.size(); ii++) {
        if (ii == iKey) {
          continue;
        }

        if (schema.getColumn_families().get(htColumnFamilies.get(ii)) == null) {
          throw new MetaException("Column Family " + htColumnFamilies.get(ii)
              + " is not defined in Hypertable table " + tblName);
        }
      }
    } catch (Exception ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    } finally {
      if (mNamespaceId != 0 && mClient != null) {
        try {
          mClient.close_namespace(mNamespaceId);
        } catch (Exception ie) {
          throw new MetaException(StringUtils.stringifyException(ie));
        }
      }
    }

  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException {
    boolean isExternal = MetaStoreUtils.isExternalTable(table);
    String tableName = getHTTableName(table);
    try {
      if (!isExternal) {
        throw new MetaException("Hypertable Storage handler only supports external" +
                                " tables currently.");
        }
    } catch (Exception ie) {
      throw new MetaException(StringUtils.stringifyException(ie));
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException {
    // nothing to do
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveHTInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveHTOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass() {
    return HTSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return this;
  }

  @Override
  public Configuration getConf() {
    return mConf;
  }

  @Override
  public void setConf(Configuration conf) {
    mConf = conf;
  }

  @Override
  public void configureTableJobProperties(
    TableDesc tableDesc,
    Map<String, String> jobProperties) {

    Properties tableProperties = tableDesc.getProperties();

    jobProperties.put(
      HTSerDe.HT_COL_MAPPING,
      tableProperties.getProperty(HTSerDe.HT_COL_MAPPING));

    String namespace =
      tableProperties.getProperty(HTSerDe.HT_NAMESPACE);
    if (namespace == null) {
      namespace = tableProperties.getProperty(Constants.META_TABLE_DB);
    }
    jobProperties.put(HTSerDe.HT_NAMESPACE, namespace);
    jobProperties.put(RowOutputFormat.NAMESPACE, namespace);

    String tableName =
      tableProperties.getProperty(HTSerDe.HT_TABLE_NAME);
    if (tableName == null) {
      tableName =
        tableProperties.getProperty(Constants.META_TABLE_NAME);
    }
    jobProperties.put(HTSerDe.HT_TABLE_NAME, tableName);

    jobProperties.put(RowOutputFormat.TABLE, tableName);
  }
}
