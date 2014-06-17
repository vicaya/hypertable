/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#ifndef HYPERTABLE_DISCRETERANDOMGENERATORUNIFORM_H
#define HYPERTABLE_DISCRETERANDOMGENERATORUNIFORM_H

#include "Common/Compat.h"

#include "DiscreteRandomGenerator.h"

namespace Hypertable {

  /**
   * Generate samples from Uniform distribution
   */
  class DiscreteRandomGeneratorUniform: public DiscreteRandomGenerator {
  public:
    DiscreteRandomGeneratorUniform() : DiscreteRandomGenerator() { }
    virtual uint64_t get_sample() { 
      uint64_t rval = ((uint64_t)m_rng() << 32) | m_rng();
      if (m_pool_max != 0)
	rval = m_pool_min + (rval % (m_pool_max-m_pool_min));
      return rval;
    }
  };

}

#endif // HYPERTABLE_DISCRETERANDOMGENERATORUNIFORM_H
