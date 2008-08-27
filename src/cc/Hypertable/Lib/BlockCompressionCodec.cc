/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "BlockCompressionCodec.h"

using namespace Hypertable;

namespace {
  const char *algo_names[BlockCompressionCodec::COMPRESSION_TYPE_LIMIT] = {
    "none",
    "bmz",
    "zlib",
    "lzo",
    "quicklz"
  };
}

const char *BlockCompressionCodec::get_compressor_name(uint16_t algo) {
  if (algo >= COMPRESSION_TYPE_LIMIT)
    return "invalid";
  return algo_names[algo];
}
