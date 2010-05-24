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

import java.io.IOException;
import java.lang.System;
import java.util.Random;

public abstract class DiscreteRandomGenerator {

  public DiscreteRandomGenerator(int min, int max, int seed) {
    mMinValue = min;
    mMaxValue = max;
    mRandom = new Random(seed);
  }

  public long getSample() {

    if (mCmf == null)
      generateCmf();

    int upper = mValueCount;
    int lower = 0;
    int ii;

    double rand = mRandom.nextDouble();

    // do a binary search through cmf to figure out which index in cmf
    // rand lies in. this will transform the uniform[0,1] distribution into
    // the distribution specified in mCmf
    while(true) {

      ii = (upper + lower)/2;
      if (mCmf[ii] >= rand) {
        if (ii == 0 || mCmf[ii-1] <= rand)
          break;
        else {
          upper = ii - 1;
          continue;
        }
      }
      else {
        lower = ii + 1;
        continue;
      }
    }

    return mNumbers[ii];
  }

  protected void generateCmf() {
    int ii;
    double norm_const;
    
    mValueCount = mMaxValue-mMinValue;

    mNumbers = new long [ mValueCount + 1];
    mCmf = new double [ mValueCount + 1 ];

    for (int i=0; i<mValueCount; i++)
      mNumbers[i] = mMinValue + (Math.abs(mRandom.nextLong()) % mValueCount);

    mCmf[0] = pmf(0);
    for (ii=1; ii < mValueCount+1 ;++ii) {
      mCmf[ii] = mCmf[ii-1] + pmf(ii);
    }

    norm_const = mCmf[mValueCount];
    // renormalize cmf
    for (ii=0; ii < mValueCount+1 ;++ii) {
      mCmf[ii] /= norm_const;
    }
  }

  abstract protected double pmf(long val);

  protected int mMinValue;
  protected int mMaxValue;
  protected int mValueCount;
  private Random mRandom;
  protected long [] mNumbers;
  protected double [] mCmf;
}
