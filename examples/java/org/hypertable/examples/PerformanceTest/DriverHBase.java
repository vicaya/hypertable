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

package org.hypertable.examples.PerformanceTest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import java.util.Random;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.Cell;

import org.hypertable.Common.Checksum;
import org.hypertable.Common.DiscreteRandomGeneratorZipf;
import org.hypertable.thriftgen.*;
import org.hypertable.thrift.ThriftClient;

public class DriverHBase extends Driver {

  static final Logger log = Logger.getLogger("org.hypertable.examples.PerformanceTest");

  public DriverHBase() throws IOException {
    this.conf  = new HBaseConfiguration();
    this.admin = new HBaseAdmin(this.conf);
  }

  public void setup(String tableName, Task.Type testType) {
    try {
      mTableName = tableName;
      this.table = new HTable(conf, tableName);
      this.table.setAutoFlush(false);
      this.table.setWriteBufferSize(1024*1024*12);

      if (testType == Task.Type.WRITE) {
        fillRandomDataBuffer();
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    mResult = new org.hypertable.examples.PerformanceTest.Result();
  }

  public void teardown() {
  }

  public void runTask(Task task) throws IOException {
    long randi;
    ByteBuffer keyByteBuf = ByteBuffer.allocate(8);
    byte [] keyBuf = keyByteBuf.array();
    DiscreteRandomGeneratorZipf zipf = null;
    org.apache.hadoop.hbase.io.Cell [] cells = null;

    if (task.distribution == Task.Distribution.ZIPFIAN)
      zipf = new DiscreteRandomGeneratorZipf((int)task.start, (int)task.end, 1, 0.8);

    long startTime = System.currentTimeMillis();

    if (task.type == Task.Type.WRITE) {
      Put put = null;
      byte [] value = null;

      try {
        for (long i=task.start; i<task.end; i++) {
          if (task.order == Task.Order.RANDOM) {
            keyByteBuf.clear();
            keyByteBuf.putLong(i);
            randi = Checksum.fletcher32(keyBuf, 0, 8) % task.keyCount;
            put = new Put( formatRowKey(randi, task.keySize).getBytes() );
          }
          else
            put = new Put( formatRowKey(i, task.keySize).getBytes() );
          value = new byte [ task.valueSize ];
          fillValueBuffer(value);
          put.add(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES, value);
          table.put(put);
        }
        this.table.flushCommits();
      }
      catch (Exception e) {
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.READ) {
      Get get = null;
      org.apache.hadoop.hbase.client.Result result = null;
      try {
        for (long i=task.start; i<task.end; i++) {
          if (task.order == Task.Order.RANDOM) {
            keyByteBuf.clear();
            if (task.distribution == Task.Distribution.ZIPFIAN) {
              randi = zipf.getSample();
              keyByteBuf.putLong(randi);
              randi = Checksum.fletcher32(keyBuf, 0, 8) % task.keyCount;
            }
            else {
              keyByteBuf.putLong(i);
              randi = Checksum.fletcher32(keyBuf, 0, 8) % task.keyCount;
            }
            get = new Get( formatRowKey(randi, task.keySize).getBytes() );
          }
          else
            get = new Get( formatRowKey(i, task.keySize).getBytes() );
          get.addColumn(COLUMN_FAMILY_BYTES, COLUMN_QUALIFIER_BYTES);
          result = table.get(get);
          if (result != null) {
            cells = result.getCellValues();
            if (cells != null) {
              for (Cell cell : cells) {
                mResult.itemsReturned++;
                mResult.valueBytesReturned += cell.getValue().length;
              }
            }
          }
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.SCAN) {
      Scan scan = new Scan(formatRowKey(task.start, task.keySize).getBytes(),
                           formatRowKey(task.end, task.keySize).getBytes());
      scan.setMaxVersions();
      this.table.setScannerCaching(task.scanBufferSize/(task.keySize+10+task.valueSize));
      ResultScanner scanner = table.getScanner(scan);
      org.apache.hadoop.hbase.client.Result result = null;

      result = scanner.next();
      while (result != null) {
        cells = result.getCellValues();
        if (cells != null) {
          for (Cell cell : cells) {
            mResult.itemsReturned++;
            mResult.valueBytesReturned += cell.getValue().length;
          }
        }
        result = scanner.next();
      }
      scanner.close();
    }
    
    mResult.itemsSubmitted += (task.end-task.start);
    mResult.elapsedMillis += System.currentTimeMillis() - startTime;
  }

  protected volatile HBaseConfiguration conf;
  protected HBaseAdmin admin;
  protected HTable table;
  protected String mTableName;
}
