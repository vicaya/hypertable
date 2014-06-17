/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANDOM_H
#define HYPERTABLE_RANDOM_H

#include <boost/random.hpp>
#include <boost/random/uniform_01.hpp>

namespace Hypertable {

  class Random {

  public:
    static void seed(unsigned int s) { ms_rng.seed((uint32_t)s); }

    static void fill_buffer_with_random_ascii(char *buf, size_t len);

    static void fill_buffer_with_random_chars(char *buf, size_t len, const char *charset);

    static uint32_t number32() { return ms_rng(); }

    static int64_t number64() {
      return ( (((int64_t)ms_rng())<<32) | ((int64_t)ms_rng()) );
    }
    static double uniform01();

    static boost::mt19937 ms_rng;
  };

}

#endif // HYPERTABLE_RANDOM_H
