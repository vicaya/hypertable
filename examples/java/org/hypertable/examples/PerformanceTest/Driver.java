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
import java.util.logging.Logger;
import java.util.Random;

public abstract class Driver {

  public static final String COLUMN_FAMILY = "column";
  public static final String COLUMN_QUALIFIER = "data";
  public static final byte [] COLUMN_FAMILY_BYTES = "column".getBytes();
  public static final byte [] COLUMN_QUALIFIER_BYTES = "data".getBytes();

  static final Logger log = Logger.getLogger("org.hypertable.examples.PerformanceTest");

  public abstract void setup(String tableName, Task.Type testType);

  public abstract void teardown();

  public abstract void runTask(Task task) throws IOException;

  public Result getResult() { return mResult; }

  protected void fillRandomDataBuffer() {
    mRandomData = new byte [ 262144 ];
    mRandom.nextBytes(mRandomData);
  }

  protected void fillValueBuffer(byte [] value) {
    int randOffset = mRandom.nextInt(mRandomData.length - 2*value.length);
    System.arraycopy(mRandomData, randOffset, value, 0, value.length);
  }

  /*
   */
  protected static String formatRowKey(final long number, final int digits) {
    StringBuilder str = new StringBuilder(digits+1);
    long d = Math.abs(number);
    str.setLength(digits);
    for (int ii = digits - 1; ii >= 0; ii--) {
      str.setCharAt(ii, (char)((d % 10) + '0'));
      d /= 10;
    }
    return str.toString();
  }

  protected static void formatRowKey(final long number, final int digits, byte [] dest) {
    long d = Math.abs(number);
    for (int ii = digits-1; ii >= 0; ii--) {
      dest[ii] = (byte)((d % 10) + '0');
      d /= 10;
    }
  }

  protected byte [] mRandomData;
  protected Result mResult;
  private Random mRandom = new Random(System.currentTimeMillis());
}
