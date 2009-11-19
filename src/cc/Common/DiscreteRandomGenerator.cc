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
#include "Common/Compat.h"
#include "Common/Logger.h"

#include "DiscreteRandomGenerator.h"

using namespace Hypertable;

DiscreteRandomGenerator::DiscreteRandomGenerator()
  : m_u01(boost::mt19937()), m_seed(1), m_value_count(0),
    m_pool_min(0), m_pool_max(0), m_numbers(0), m_cmf(0)
{
  m_rng.seed((uint32_t)m_seed);
  m_u01 = boost::uniform_01<boost::mt19937>(m_rng);
}


uint64_t DiscreteRandomGenerator::get_sample()
{
  if (m_cmf == 0) {
    HT_ASSERT(m_value_count || m_pool_max);
    if (m_value_count == 0)
      m_value_count = m_pool_max;
    else if (m_pool_max == 0)
      m_pool_max = m_value_count;
    m_numbers = new uint64_t [ m_value_count ];
    m_cmf = new double [ m_value_count + 1 ];
    generate_cmf();
  }

  uint64_t upper = m_value_count;
  uint64_t lower = 0;
  uint64_t ii;

  double rand = m_u01();

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

  return m_numbers[ii];

}

void DiscreteRandomGenerator::generate_cmf()
{
  uint64_t ii;
  double norm_const;

  if (m_value_count == m_pool_max) {
    uint64_t temp_num, index;
    for (uint64_t i=0; i<m_value_count; i++)
      m_numbers[i] = i;
    // randomize the array of numbers
    for (uint64_t i=0; i<m_value_count; i++) {
      index = (uint64_t)m_rng() % m_value_count;
      temp_num = m_numbers[0];
      m_numbers[0] = m_numbers[index];
      m_numbers[index] = temp_num;
    }
  }
  else {
    uint64_t pool_diff = m_pool_max - m_pool_min;
    for (uint64_t i=0; i<m_value_count; i++)
      m_numbers[i] = m_pool_min + ((uint64_t)m_rng() % pool_diff);
  }

  m_cmf[0] = pmf(0);
  for (ii=1; ii < m_value_count+1 ;++ii) {
    m_cmf[ii] = m_cmf[ii-1] + pmf(ii);
  }
  norm_const = m_cmf[m_value_count];
  // renormalize cmf
  for (ii=0; ii < m_value_count+1 ;++ii) {
    m_cmf[ii] /= norm_const;
  }
}


