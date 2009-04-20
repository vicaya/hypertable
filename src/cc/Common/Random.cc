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
#include "Common/Compat.h"
#include "Common/Logger.h"

#include "Random.h"

namespace {
  const char cb64[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
}

using namespace Hypertable;

boost::mt19937 Random::ms_rng;

void Random::fill_buffer_with_random_ascii(char *buf, size_t len) {
  size_t in_i=0, out_i=0;
  uint32_t u32;
  uint8_t *in;

  while (out_i < len) {

    u32 = number32();
    in = (uint8_t *)&u32;
    in_i = 0;

    buf[out_i++] = cb64[ in[in_i] >> 2 ];
    if (out_i == len)
      break;

    buf[out_i++] = cb64[((in[in_i] & 0x03) << 4) | ((in[in_i+1] & 0xf0) >> 4)];
    if (out_i == len)
      break;

    buf[out_i++] = cb64[((in[in_i+1] & 0x0f) << 2)|((in[in_i+2] & 0xc0) >> 6)];
    if (out_i == len)
      break;

    buf[out_i++] = cb64[ in[in_i+2] & 0x3f ];

    in_i += 3;
  }

}


void Random::fill_buffer_with_random_chars(char *buf, size_t len, const char *charset) {
  size_t in_i=0, out_i=0;
  uint32_t u32;
  uint8_t *in;
  size_t set_len = strlen(charset);

  HT_ASSERT(set_len > 0 && set_len <= 256);

  while (out_i < len) {

    u32 = number32();
    in = (uint8_t *)&u32;

    in_i = 0;
    buf[out_i++] = charset[ in[in_i] % set_len ];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = charset[ in[in_i] % set_len ];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = charset[ in[in_i] % set_len ];
    if (out_i == len)
      break;

    in_i++;
    buf[out_i++] = charset[ in[in_i] % set_len ];

  }
}

double Random::uniform01() {
  /**
   * u01 uses a copy of ms_rng, this means the state of ms_rng does not get updated
   * and other calls using ms_rng will re-use random numbers used in the generation of
   * randoms from u01. In practice this should not be an issue unless another method uses
   * uniform_01 generated using ms_rng (in which case it will generate the same numbers as
   * this method). u01 needs to be static so that its state is updated & resued across calls
   * (ow it will generate the same number every time).
   */
  static boost::uniform_01<boost::mt19937> u01(ms_rng);
  return u01();
}

