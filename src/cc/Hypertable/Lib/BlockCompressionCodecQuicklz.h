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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECQUICKLZ_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECQUICKLZ_H

#include "Common/Compat.h"
#include "BlockCompressionCodec.h"
#include "quicklz/quicklz.h"

namespace Hypertable {

  class BlockCompressionCodecQuicklz : public BlockCompressionCodec {

  public:
    BlockCompressionCodecQuicklz(const Args &args);
    virtual ~BlockCompressionCodecQuicklz();

    virtual void deflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header, size_t reserve=0);
    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header);
    virtual int get_type() { return QUICKLZ; }

  private:
    qlz_state_compress m_compress;
    qlz_state_decompress m_decompress;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECQUICKLZ_H

