/**
 * Copyright (C) 2007 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <boost/algorithm/string.hpp>
#include "CompressorFactory.h"
#include "BlockCompressionCodecBmz.h"
#include "BlockCompressionCodecNone.h"
#include "BlockCompressionCodecZlib.h"
#include "BlockCompressionCodecLzo.h"
#include "BlockCompressionCodecQuicklz.h"

using namespace Hypertable;
using namespace std;
using namespace boost;

BlockCompressionCodec::Type
CompressorFactory::parse_block_codec_spec(const string &spec,
                                          BlockCompressionCodec::Args &args) {
  string name;

  split(args, spec, is_any_of(" \t"), token_compress_on);

  if (!args.empty()) {
    name = args.front();
    args.erase(args.begin());
  }

  if (name == "none" || name.empty())
    return BlockCompressionCodec::NONE;

  if (name == "bmz")
    return BlockCompressionCodec::BMZ;

  if (name == "zlib")
    return BlockCompressionCodec::ZLIB;

  if (name == "lzo")
    return BlockCompressionCodec::LZO;

  if (name == "quicklz")
    return BlockCompressionCodec::QUICKLZ;

  HT_ERRORF("unknown codec type: %s", name.c_str());
  return BlockCompressionCodec::UNKNOWN;
}

BlockCompressionCodec *
CompressorFactory::create_block_codec(BlockCompressionCodec::Type type,
                                      const BlockCompressionCodec::Args &args) {
  switch (type) {
  case BlockCompressionCodec::BMZ:
    return new BlockCompressionCodecBmz(args);
  case BlockCompressionCodec::NONE:
    return new BlockCompressionCodecNone(args);
  case BlockCompressionCodec::ZLIB:
    return new BlockCompressionCodecZlib(args);
  case BlockCompressionCodec::LZO:
    return new BlockCompressionCodecLzo(args);
  case BlockCompressionCodec::QUICKLZ:
    return new BlockCompressionCodecQuicklz(args);
  default:
    return NULL;
  }
}
