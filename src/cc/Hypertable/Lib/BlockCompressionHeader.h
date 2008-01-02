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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADER_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

namespace Hypertable {

  /**
   * Base class for compressed block header.
   */
  class BlockCompressionHeader {
  public:
    virtual ~BlockCompressionHeader() { return; }

    void set_magic(const char magic[12]) { memcpy(m_magic, magic, 12); }
    void get_magic(char magic[12]) { memcpy(magic, m_magic, 12); }
    bool check_magic(const char magic[12]) { return !memcmp(magic, m_magic, 12); }

    void     set_length(uint32_t length) { m_length = length; }
    uint32_t get_length() { return m_length; }

    void     set_zlength(uint32_t zlength) { m_zlength = zlength; }
    uint32_t get_zlength() { return m_zlength; }

    void     set_checksum(uint16_t checksum) { m_checksum = checksum; }
    uint16_t get_checksum() { return m_checksum; }

    void     set_flags(uint16_t flags) { m_flags = flags; }
    void     set_flag(uint16_t flag) { m_flags |= flag; }
    void     unset_flag(uint16_t flag) { m_flags &= ~flag; }
    uint16_t get_flags() { return m_flags; }

    virtual size_t encoded_length() = 0;
    virtual void   encode(uint8_t **buf_ptr) = 0;
    virtual int    decode(uint8_t **buf_ptr, size_t *remaining_ptr) = 0;

    enum Flags { FLAGS_COMPRESSED=0x0001 };

  protected:
    char m_magic[12];
    uint32_t m_length;
    uint32_t m_zlength;
    uint16_t m_checksum;
    uint16_t m_flags;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

