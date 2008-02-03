/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H

extern "C" {
#include <zlib.h>
}

#include "BlockCompressionCodec.h"

namespace Hypertable {

  class BlockCompressionCodecZlib : public BlockCompressionCodec {

  public:
    BlockCompressionCodecZlib(const Args &args);
    virtual ~BlockCompressionCodecZlib();

    virtual int set_args(const Args &args);
    virtual int deflate(const DynamicBuffer &input, DynamicBuffer &output,
                        BlockCompressionHeader &header, size_t reserve=0);
    virtual int inflate(const DynamicBuffer &input, DynamicBuffer &output,
                        BlockCompressionHeader &header);
    virtual int get_type() { return ZLIB; }

  private:
    z_stream  m_stream_inflate;
    bool      m_inflate_initialized;
    z_stream  m_stream_deflate;
    bool      m_deflate_initialized;
    int       m_level;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECZLIB_H

