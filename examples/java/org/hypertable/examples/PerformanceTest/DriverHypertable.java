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
import java.util.Vector;

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
  }

  protected void finalize() {
    if (mParallelism == 0) {
      try {
        if (mNamespaceId != 0)
          mClient.close_namespace(mNamespaceId);
      }
      catch (Exception e) {
        System.out.println("Unable to close namespace - " + mNamespace +
                           e.getMessage());
        System.exit(-1);
      }
    }
  }

  public void setup(String tableName, Task.Type testType, int parallelism) {
    mParallelism = parallelism;
    mTableName = tableName;

    if (mParallelism == 0) {
      mCellsWriter = new SerializedCellsWriter(CLIENT_BUFFER_SIZE);
      try {
        mClient = ThriftClient.create("localhost", 38080);
        mNamespace = "/";
        mNamespaceId = mClient.open_namespace(mNamespace);
      }
      catch (Exception e) {
        System.out.println("Unable to establish connection to ThriftBroker at localhost:38080 " +
                           "and open namespace '/'- " +
                           e.getMessage());
        System.exit(-1);
      }
    }

    try {

      mCommon.fillRandomDataBuffer();

      if (mParallelism > 0) {
        mThreadStates = new DriverThreadState[mParallelism];
        for (int i=0; i<mParallelism; i++) {
          mThreadStates[i] = new DriverThreadState();
          mThreadStates[i].common = mCommon;
          mThreadStates[i].thread = new DriverThreadHypertable("/", mTableName, mThreadStates[i]);
          mThreadStates[i].thread.start();
        }
      }
      else {
        mCellsWriter.clear();
        if (testType == Task.Type.WRITE || testType == Task.Type.INCR)
          mMutator = mClient.open_mutator(mNamespaceId, mTableName, 0, 0);
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
      byte [] family = mCommon.COLUMN_FAMILY_BYTES;
      byte [] qualifier = mCommon.COLUMN_QUALIFIER_BYTES;
      byte [] value = new byte [ task.valueSize ];

      if (mParallelism == 0)
        mCellsWriter.clear();

      try {
        for (long i=task.start; i<task.end; i++) {
          if (task.order == Task.Order.RANDOM) {
            if (task.keyCount > Integer.MAX_VALUE)
              randi = mCommon.random.nextLong();
            else
              randi = mCommon.random.nextInt();
            if (task.keyMax != -1)
              randi %= task.keyMax;
            mCommon.formatRowKey(randi, task.keySize, row);
          }
          else
            mCommon.formatRowKey(i, task.keySize, row);
          mCommon.fillValueBuffer(value);
          if (mParallelism > 0) {
            mCellsWriter = new SerializedCellsWriter(task.keySize + family.length + qualifier.length + value.length + 32);
            if (!mCellsWriter.add(row, 0, task.keySize, family, 0, family.length, qualifier, 0, qualifier.length,
                                  SerializedCellsFlag.AUTO_ASSIGN, value, 0, value.length, SerializedCellsFlag.FLAG_INSERT)) {
              System.out.println("Failed to write to SerializedCellsWriter");
              System.exit(-1);
            }
            synchronized (mThreadStates[mThreadNext]) {
              mThreadStates[mThreadNext].updates.add(mCellsWriter);
              mThreadStates[mThreadNext].notifyAll();
            }
            mThreadNext = (mThreadNext + 1) % mParallelism;
            mCellsWriter = null;
          }
          else {
            while (!mCellsWriter.add(row, 0, task.keySize,
                                     family, 0, family.length,
                                     qualifier, 0, qualifier.length,
                                     SerializedCellsFlag.AUTO_ASSIGN,
                                     value, 0, value.length, SerializedCellsFlag.FLAG_INSERT)) {
              mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), false);
              mCellsWriter.clear();
            }
          }
        }
        if (mParallelism == 0) {
          if (!mCellsWriter.isEmpty())
            mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), true);
          else
            mClient.flush_mutator(mMutator);
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.INCR) {
      byte [] row = new byte [ task.keySize ];
      byte [] family = mCommon.COLUMN_FAMILY_BYTES;
      byte [] qualifier = mCommon.COLUMN_QUALIFIER_BYTES;
      byte [] value = mCommon.INCREMENT_VALUE_BYTES;

      if (mParallelism == 0)
        mCellsWriter.clear();

      try {
        for (long i=task.start; i<task.end; i++) {
          if (task.order == Task.Order.RANDOM) {
            if (task.keyCount > Integer.MAX_VALUE)
              randi = mCommon.random.nextLong();
            else
              randi = mCommon.random.nextInt();
            if (task.keyMax != -1)
              randi %= task.keyMax;
            mCommon.formatRowKey(randi, task.keySize, row);
          }
          else
            mCommon.formatRowKey(i, task.keySize, row);
          if (mParallelism > 0) {
            mCellsWriter = new SerializedCellsWriter(task.keySize + family.length + qualifier.length + value.length + 32);
            if (!mCellsWriter.add(row, 0, task.keySize, family, 0, family.length, qualifier, 0, qualifier.length,
                                  SerializedCellsFlag.AUTO_ASSIGN, value, 0, value.length, SerializedCellsFlag.FLAG_INSERT)) {
              System.out.println("Failed to write to SerializedCellsWriter");
              System.exit(-1);
            }
            synchronized (mThreadStates[mThreadNext]) {
              mThreadStates[mThreadNext].updates.add(mCellsWriter);
              mThreadStates[mThreadNext].notifyAll();
            }
            mThreadNext = (mThreadNext + 1) % mParallelism;
            mCellsWriter = null;
          }
          else {
            while (!mCellsWriter.add(row, 0, task.keySize,
                                     family, 0, family.length,
                                     qualifier, 0, qualifier.length,
                                     SerializedCellsFlag.AUTO_ASSIGN,
                                     value, 0, value.length, SerializedCellsFlag.FLAG_INSERT)) {
              mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), false);
              mCellsWriter.clear();
            }
          }
        }
        if (mParallelism == 0) {
          if (!mCellsWriter.isEmpty())
            mClient.set_cells_serialized(mMutator, mCellsWriter.buffer(), true);
          else
            mClient.flush_mutator(mMutator);
        }
      }
      catch (Exception e) {
        e.printStackTrace();
        log.severe(e.toString());
        throw new IOException("Unable to set cell via thrift - " + e.toString());
      }
    }
    else if (task.type == Task.Type.READ) {
      String row;
      SerializedCellsReader reader = new SerializedCellsReader(null);

      if (mParallelism != 0) {
        System.out.println("Parallel reads not implemented");
        System.exit(1);
      }

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
            row = mCommon.formatRowKey(randi, task.keySize);
          }
          else
            row = mCommon.formatRowKey(i, task.keySize);

          reader.reset( mClient.get_row_serialized(mNamespaceId, mTableName, row) );
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
      String start_row = mCommon.formatRowKey(task.start, task.keySize);
      String end_row = mCommon.formatRowKey(task.end, task.keySize);
      SerializedCellsReader reader = new SerializedCellsReader(null);

      if (mParallelism != 0) {
        System.out.println("Parallel scans not implemented");
        System.exit(1);
      }

      ScanSpec scan_spec = new ScanSpec();
      RowInterval ri = new RowInterval();

      ri.setStart_row(start_row);
      ri.setStart_inclusive(true);
      ri.setEnd_row(end_row);
      ri.setEnd_inclusive(false);
      scan_spec.addToRow_intervals(ri);

      try {
        long scanner = mClient.open_scanner(mNamespaceId, mTableName, scan_spec, true);
        while (!eos) {
          reader.reset( mClient.next_cells_serialized(scanner) );
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

    try {
      for (int i=0; i<mParallelism; i++) {
        synchronized (mThreadStates[mThreadNext]) {
          if (!mThreadStates[mThreadNext].updates.isEmpty()) {
            mThreadStates[mThreadNext].wait();
          }
          // wait for read requests here
        }
      }
    }
    catch (InterruptedException e) {
      e.printStackTrace();
      System.exit(1);
    }

    mResult.itemsSubmitted += (task.end-task.start);
    mResult.elapsedMillis += System.currentTimeMillis() - startTime;
  }

  ThriftClient mClient;
  String mNamespace;
  String mTableName;
  long mMutator;
  long mNamespaceId=0;
  SerializedCellsWriter mCellsWriter;
  int mParallelism=0;
  DriverThreadState [] mThreadStates;
  int mThreadNext = 0;
}
