/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H
#define HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H

#include "Common/DynamicBuffer.h"
#include "BlockCompressionCodec.h"

namespace Hypertable {

class BlockCompressionCodecBmz : public BlockCompressionCodec {
public:
  BlockCompressionCodecBmz(const Args &args);
  virtual ~BlockCompressionCodecBmz();

  virtual int set_args(const Args &args);
  virtual int deflate(const DynamicBuffer &input, DynamicBuffer &output,
                      BlockCompressionHeader &header, size_t reserve=0);
  virtual int inflate(const DynamicBuffer &input, DynamicBuffer &output,
                      BlockCompressionHeader &header);
  virtual int get_type() { return BMZ; }

private:
  DynamicBuffer m_workmem;
  size_t        m_offset;
  size_t        m_fp_len;
};

} // namespace Hypertable

#endif // HYPERTABLE_BLOCKCOMPRESSIONCODECBMZ_H
