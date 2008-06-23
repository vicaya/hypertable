/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODEC_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODEC_H

#include <vector>
#include "Common/Thread.h"
#include "Common/Error.h"
#include "Common/ReferenceCount.h"

#include "BlockCompressionHeader.h"

namespace Hypertable {

  class DynamicBuffer;

  /**
   * Abstract base class for block compression codecs.
   */
  class BlockCompressionCodec : public ReferenceCount {
  public:
    enum Type { UNKNOWN=-1, NONE=0, BMZ=1, ZLIB=2, LZO=3, QUICKLZ=4,
                COMPRESSION_TYPE_LIMIT=5 };
    typedef std::vector<String> Args;

    static const char *get_compressor_name(uint16_t algo);

    BlockCompressionCodec() { HT_THREAD_ID_SET(m_creator_thread); }
    virtual ~BlockCompressionCodec() { return; }

    virtual void deflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header, size_t reserve=0) = 0;

    virtual void inflate(const DynamicBuffer &input, DynamicBuffer &output,
                         BlockCompressionHeader &header) = 0;

    virtual void set_args(const Args &args) {}

    virtual int get_type() = 0;

    HT_THREAD_ID_DECL(m_creator_thread);
  };
  typedef boost::intrusive_ptr<BlockCompressionCodec> BlockCompressionCodecPtr;


} // namespace Hypertable

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODEC_H
