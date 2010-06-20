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

import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.TException;

import org.hypertable.Common.DiscreteRandomGeneratorZipf;
import org.hypertable.Common.Checksum;
import org.hypertable.thriftgen.*;
import org.hypertable.thrift.SerializedCellsFlag;
import org.hypertable.thrift.SerializedCellsReader;
import org.hypertable.thrift.SerializedCellsWriter;
import org.hypertable.thrift.ThriftClient;

public class DriverHypertable extends Driver {

  static final Logger log = Logger.getLogger("org.hypertable.examples.PerformanceTest");

  public static final int CLIENT_BUFFER_SIZE = 1024*1024*12;

  public DriverHypertable() throws TTransportException, TException {
    mCellsWriter = new SerializedCellsWriter(CLIENT_BUFFER_SIZE);
    try {
      mClient = ThriftClient.create("localhost", 38080);
    }
    catch (Exception e) {
      System.out.println("Unable to establish connection to ThriftBroker at localhost:38080 - " +
                         e.getMessage());
      System.exit(-1);
    }
  }

  public void setup(String tableName, Task.Type testType) {
    try {
      mTableName = tableName;
      mCellsWriter.clear();
      if (testType == Task.Type.WRITE) {
        mMutator = mClient.open_mutator(mTableName, 0, 0);
        fillRandomDataBuffer();
      }
    }
    catch (ClientException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    catch (TException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    mResult = new Result();
  }

  public void teardown() {
  }

  public void runTask(Task task) throws IOException {
    long randi;
    ByteBuffer keyByteBuf = ByteBuffer.allocate(8);
    byte [] keyBuf = keyByteBuf.array();
    DiscreteRandomGeneratorZipf zipf = null;

    if (task.distribution == Task.Distribution.ZIPFIAN)
      zipf = new DiscreteRandomGeneratorZipf((int)task.start, (int)task.end, 1, 0.8);

    long startTime = System.currentTimeMillis();
    
    if (task.type == Task.Type.WRITE) {
      byte [] row = new byte [ task.keySize ];
      byte [] family = COLUMN_FAMILY_BYTES;
      byte [] qualifier = COLUMN_QUALIFIER_BYTES;
      byte [] value = new byte [ task.valueSize ];

      mCellsWriter.clear();

      try {
        for (long i=task.start; i<task.end; i++) {
          if (task.order == Task.Order.RANDOM) {
            keyByteBuf.clear();
            keyByteBuf.putLong(i);
            randi = Checksum.fletcher32(keyBuf, 0, 8) % task.keyCount;
            formatRowKey(randi, task.keySize, row);
          }
          else
            formatRowKey(i, task.keySize, row);
          fillValueBuffer(value);
          while (!mCellsWriter.add(row, 0, task.keySize,
                                   family, 0, family.length,
                                   qualifier, 0, qualifier.length,
                                   SerializedCellsFlag.AUTO_ASSIGN,
                                   value, 0, value.length)) {
            mClient.set_cells_serialized(mMutator, mCellsWriter.array(), false);
            mCellsWriter.clear();
          }
        }
        if (!mCellsWriter.isEmpty())
          mClient.set_cells_serialized(mMutator, mCellsWriter.array(), true);
        else
          mClient.flush_mutator(mMutator);
      }
      catch (Exception e) {
        e.printStackTrace();
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.READ) {
      byte [] buffer;
      String row;
      SerializedCellsReader reader = new SerializedCellsReader(null);

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
            row = formatRowKey(randi, task.keySize);
          }
          else
            row = formatRowKey(i, task.keySize);

          buffer = mClient.get_row_serialized(mTableName, row);
          reader.reset(buffer);
          while (reader.next()) {
            mResult.itemsReturned++;
            mResult.valueBytesReturned += reader.get_value_length();
          }
        }
      }
      catch (Exception e) {
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.SCAN) {
      boolean eos = false;
      String start_row = formatRowKey(task.start, task.keySize);
      String end_row = formatRowKey(task.end, task.keySize);
      SerializedCellsReader reader = new SerializedCellsReader(null);
      byte [] buffer;

      ScanSpec scan_spec = new ScanSpec();
      RowInterval ri = new RowInterval();

      ri.setStart_row(start_row);
      ri.setStart_inclusive(true);
      ri.setEnd_row(end_row);
      ri.setEnd_inclusive(false);
      scan_spec.addToRow_intervals(ri);

      try {
        long scanner = mClient.open_scanner(mTableName, scan_spec, true);
        while (!eos) {
          buffer = mClient.next_cells_serialized(scanner);
          reader.reset(buffer);
          while (reader.next()) {
            mResult.itemsReturned++;
            mResult.valueBytesReturned += reader.get_value_length();
          }
          eos = reader.eos();
        }
        mClient.close_scanner(scanner);
      }
      catch (Exception e) {
        e.printStackTrace();
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    
    mResult.itemsSubmitted += (task.end-task.start);
    mResult.elapsedMillis += System.currentTimeMillis() - startTime;
  }

  ThriftClient mClient;
  String mTableName;
  long mMutator;
  SerializedCellsWriter mCellsWriter;
}
