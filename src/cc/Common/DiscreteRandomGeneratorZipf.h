/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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
#ifndef HYPERTABLE_DISCRETERANDOMGENERATORZIPF_H
#define HYPERTABLE_DISCRETERANDOMGENERATORZIPF_H

#include "Common/Compat.h"

#include <vector>
#include <cmath>

#include "DiscreteRandomGenerator.h"

namespace Hypertable {

  /**
   * Generate samples from Zipf distribution http://en.wikipedia.org/wiki/Zipf%27s_law
   *
   * Designed for case where parameter 0 < s < 1, under which condition the probability of
   * the number k (ie of rank k) occuring is (www.icis.ntu.edu.sg/scs-ijit/1204/1204_6.pdf):
   * Pk = C/k^s where C is approximated by (1-s)/(N^(1-s))
   *
   * In this class, m_s replaces s, m_C replaces C and m_max_val replaces N
   */
  class DiscreteRandomGeneratorZipf: public DiscreteRandomGenerator {
  public:
    DiscreteRandomGeneratorZipf(double s = 0.8);
    /**
     * Returns the probability of generating val+1 from this distribution
     * Uses val+1 because dist. pmf is undefined at 0.
     * Works for the range [0, max_val]
     * @return probability of generating val+1 from this distribution
     */
    double pmf(uint64_t val);

  private:
    bool m_initialized;
    double m_s;
    double m_norm;
  };

} // namespace Hypertable

#endif // HYPERTABLE_DISCRETERANDOMGENERATORZIPF_H
