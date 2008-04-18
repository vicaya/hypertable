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

#ifndef HYPERTABLE_BLOCKCOMPRESSIONHEADER_H
#define HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

namespace Hypertable {

  /**
   * Base class for compressed block header.
   */
  class BlockCompressionHeader {
  public:

    static const size_t LENGTH = 26;

    BlockCompressionHeader() : m_data_length(0), m_data_zlength(0), m_data_checksum(0), m_compression_type(-1)
    { return; }

    BlockCompressionHeader(const char *magic) : m_data_length(0), m_data_zlength(0), m_data_checksum(0), m_compression_type(-1)
      { memcpy(m_magic, magic, 10); }

    virtual ~BlockCompressionHeader() { return; }

    void set_magic(const char magic[10]) { memcpy(m_magic, magic, 10); }
    void get_magic(char magic[10]) { memcpy(magic, m_magic, 10); }
    bool check_magic(const char magic[10]) { return !memcmp(magic, m_magic, 10); }

    void     set_data_length(uint32_t length) { m_data_length = length; }
    uint32_t get_data_length() { return m_data_length; }

    void     set_data_zlength(uint32_t zlength) { m_data_zlength = zlength; }
    uint32_t get_data_zlength() { return m_data_zlength; }

    void     set_data_checksum(uint32_t checksum) { m_data_checksum = checksum; }
    uint32_t get_data_checksum() { return m_data_checksum; }

    void     set_compression_type(uint16_t type) { m_compression_type = type; }
    uint16_t get_compression_type() { return m_compression_type; }

    virtual size_t length() { return LENGTH; }
    virtual void   encode(uint8_t **buf_ptr);
    virtual void   write_header_checksum(uint8_t *base, uint8_t **buf_ptr);
    virtual int    decode(uint8_t **buf_ptr, size_t *remaining_ptr);

  protected:
    char m_magic[10];
    uint32_t m_data_length;
    uint32_t m_data_zlength;
    uint32_t m_data_checksum;
    uint16_t m_compression_type;
  };

}

#endif // HYPERTABLE_BLOCKCOMPRESSIONHEADER_H

