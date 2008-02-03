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

#ifndef HYPERTABLE_COMPRESSOR_FACTORY_H
#define HYPERTABLE_COMPRESSOR_FACTORY_H

#include "BlockCompressionCodec.h"

namespace Hypertable {

class CompressorFactory {
public:
  /**
   * Given a block codec config string return its type and put 
   * config 
   */
  static BlockCompressionCodec::Type
  parse_block_codec_spec(const std::string& spec,
                         BlockCompressionCodec::Args &args);

  static BlockCompressionCodec *
  create_block_codec(BlockCompressionCodec::Type,
                     const BlockCompressionCodec::Args &args =
                     BlockCompressionCodec::Args());

  static BlockCompressionCodec *
  create_block_codec(const std::string& spec) {
    BlockCompressionCodec::Args args;
    return create_block_codec(parse_block_codec_spec(spec, args));
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_COMPRESSOR_FACTORY_H
