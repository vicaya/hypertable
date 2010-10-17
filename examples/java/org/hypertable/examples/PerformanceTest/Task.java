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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Task {

  public enum Type { READ, WRITE, SCAN, INCR }

  public enum Order { SEQUENTIAL, RANDOM }

  public enum Distribution { UNIFORM, ZIPFIAN }

  public Task() { }

  public Task(Type t, long kmax, int ksize, int vsize, long kc,
              Order o, Distribution d, long s, long e, int sbsize) {
    type = t;
    keyMax = kmax;
    keySize = ksize;
    valueSize = vsize;
    keyCount = kc;
    order = o;
    distribution = d;
    start = s;
    end = e;
    scanBufferSize = sbsize;
  }

  public int encodedLength() { return 56; }

  public void encode(ByteBuffer buf) {
    buf.putInt(type.ordinal());
    buf.putLong(keyMax);
    buf.putInt(keySize);
    buf.putInt(valueSize);
    buf.putLong(keyCount);
    buf.putInt(order.ordinal());
    buf.putInt(distribution.ordinal());
    buf.putLong(start);
    buf.putLong(end);
    buf.putInt(scanBufferSize);
  }

  public void decode(ByteBuffer buf) {
    type  = Type.values()[buf.getInt()];
    keyMax = buf.getLong();
    keySize = buf.getInt();
    valueSize = buf.getInt();
    keyCount = buf.getLong();
    order = Order.values()[buf.getInt()];
    distribution = Distribution.values()[buf.getInt()];
    start = buf.getLong();
    end   = buf.getLong();
    scanBufferSize = buf.getInt();
  }

  public String toString() {
    return new String("type=" + type + 
                      ", keyMax=" + keyMax +
                      ", keySize=" + keySize +
                      ", valueSize=" + valueSize +
                      ", keyCount=" + keyCount +
                      ", order=" + order + ", distribution=" + distribution +
                      ", start=" + start + ", end=" + end + 
                      ", scanBufferSize=" + scanBufferSize + ")");
  }

  public Type type;
  public Order order;
  public Distribution distribution = Distribution.UNIFORM;
  public long start;
  public long end;
  public long keyCount = -1;
  public long keyMax = -1;
  public int  keySize = -1;
  public int  valueSize = -1;
  public int  scanBufferSize = 65536;
}
