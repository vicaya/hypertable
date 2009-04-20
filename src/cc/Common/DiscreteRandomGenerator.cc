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
#include "DiscreteRandomGenerator.h"

using namespace Hypertable;

DiscreteRandomGenerator::DiscreteRandomGenerator(unsigned int seed, size_t max_val)
    : m_seed(seed), m_max_val(max_val)
{
  m_rng.seed(m_seed);
  m_cmf.resize(max_val+1);
  m_generated_cmf = false;
}

size_t DiscreteRandomGenerator::get_sample()
{
  if (!m_generated_cmf)
    generate_cmf();

  size_t upper = m_cmf.size()-1;
  size_t lower = 0;
  size_t ii;

  double rand = Random::uniform01();

  assert(rand >=0.0 && rand <= 1.0);

  // do a binary search through cmf to figure out which index in cmf
  // rand lies in. this will transform the uniform[0,1] distribution into
  // the distribution specified in m_cmf
  while(true) {

    assert(upper >= lower);
    ii = (upper + lower)/2;
    if (m_cmf[ii] >= rand) {
      if (ii == 0 || m_cmf[ii-1] <= rand)
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

  return ii;

}

void DiscreteRandomGenerator::generate_cmf()
{
  size_t ii;
  double norm_const;

  m_cmf[0] = pmf(0);
  for (ii=1; ii < m_max_val+1 ;++ii) {
    m_cmf[ii] = m_cmf[ii-1] + pmf(ii);
  }
  norm_const = m_cmf[m_max_val];
  // renormalize cmf
  for (ii=0; ii < m_max_val+1 ;++ii) {
    m_cmf[ii] /= norm_const;
  }

  m_generated_cmf = true;
}


