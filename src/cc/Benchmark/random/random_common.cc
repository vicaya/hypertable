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

namespace {
  const char cb64[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
}


void fill_buffer_with_random_ascii(char *buf, size_t len) {
  size_t in_i=0, out_i=0;
  uint32_t u32;
  uint8_t *in;

  while (out_i < len) {

    u32 = (uint32_t)random();
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
