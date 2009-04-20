/**
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
#ifndef HYPERTABLE_DISCRETERANDOMGENERATOR_H
#define HYPERTABLE_DISCRETERANDOMGENERATOR_H

#include "Common/Compat.h"
#include "Random.h"
#include <boost/random/uniform_01.hpp>
#include <vector>

namespace Hypertable {
  using namespace std;
  /**
   * Generates samples from a discrete probability distribution
   * in the range [0, max_val] by transforming a
   * uniform [0,1] distribution into the desired distribution
   */
  class DiscreteRandomGenerator {
  public:
    /**
     *
     */
    DiscreteRandomGenerator(unsigned int seed, size_t max_val);

    /**
     * Returns a sample from the distribution
     *
     */
    virtual size_t get_sample();
    virtual ~DiscreteRandomGenerator() {};

  protected:

    /**
     * Generate the cumulative mass function for the distribution
     */
    virtual void generate_cmf();

    /**
     * Returns the probability of drawing a specific value from the distribution
     *
     * @param val value to be generated
     * @return probability of generating this value
     */
    virtual double pmf(size_t val) = 0;

    Random m_rng;
    unsigned int m_seed;
    size_t m_max_val;
    bool m_generated_cmf;
    vector<double> m_cmf;
  };
} // namespace Hypertable

#endif // HYPERTABLE_DISCRETERANDOMGENERATOR_H
