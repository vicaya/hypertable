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

import java.util.Random;

public class DriverCommon {

  public static final String COLUMN_FAMILY = "column";
  public static final String COLUMN_QUALIFIER = "data";
  public static final String INCREMENT_VALUE = "1";
  public static final byte [] COLUMN_FAMILY_BYTES = "column".getBytes();
  public static final byte [] COLUMN_QUALIFIER_BYTES = "data".getBytes();
  public static final byte [] INCREMENT_VALUE_BYTES = "1".getBytes();

  public void fillRandomDataBuffer() {
    randomData = new byte [ 262144 ];
    random.nextBytes(randomData);
  }

  public void fillValueBuffer(byte [] value) {
    int randOffset = random.nextInt(randomData.length - 2*value.length);
    System.arraycopy(randomData, randOffset, value, 0, value.length);
  }

  /*
   */
  public static String formatRowKey(final long number, final int digits) {
    StringBuilder str = new StringBuilder(digits+1);
    long d = Math.abs(number);
    str.setLength(digits);
    for (int ii = digits - 1; ii >= 0; ii--) {
      str.setCharAt(ii, (char)((d % 10) + '0'));
      d /= 10;
    }
    return str.toString();
  }

  public static void formatRowKey(final long number, final int digits, byte [] dest) {
    long d = Math.abs(number);
    for (int ii = digits-1; ii >= 0; ii--) {
      dest[ii] = (byte)((d % 10) + '0');
      d /= 10;
    }
  }

  protected byte [] randomData;
  protected Random random = new Random(System.currentTimeMillis());
}
