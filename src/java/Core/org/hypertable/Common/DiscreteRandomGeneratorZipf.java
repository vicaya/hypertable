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

package org.hypertable.Common;

import java.lang.System;

public class DiscreteRandomGeneratorZipf extends DiscreteRandomGenerator {

  public DiscreteRandomGeneratorZipf(int min, int max, int seed, double s) {
    super(min, max, seed);
    mS = s;
  }

  protected double pmf(long val) {
    if (!mInitialized) {
      mNorm = (1.0-mS)/(Math.pow((double)(mValueCount+1), 1.0-mS));
      mInitialized = true;
    }
    assert val>=0 && val <= mValueCount+1;
    val++;
    double prob = mNorm/Math.pow((double)val, mS);
    return prob;
  }

  public static void main(String args[]) {
    DiscreteRandomGeneratorZipf zipf = new DiscreteRandomGeneratorZipf(0, 10000000, 1, 0.8);

    for (int i=0; i<10000000; i++) 
      System.out.println(zipf.getSample());

  }

  private boolean mInitialized = false;
  double mS;
  double mNorm;
}