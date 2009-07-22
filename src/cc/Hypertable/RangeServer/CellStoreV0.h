/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_CELLSTOREV0_H
#define HYPERTABLE_CELLSTOREV0_H

#include <map>
#include <string>
#include <vector>

#ifdef _GOOGLE_SPARSE_HASH
#include <google/sparse_hash_set>
#else
#include <ext/hash_set>
#endif

#include "CellStoreBlockIndexMap.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "Common/BloomFilter.h"
#include "Common/BlobHashSet.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "CellStore.h"
#include "CellStoreTrailerV0.h"


/**
 * Forward declarations
 */
namespace Hypertable {
  class BlockCompressionCodec;
  class Client;
  class Protocol;
}

namespace Hypertable {

  class CellStoreV0 : public CellStore {

  public:
    CellStoreV0(Filesystem *filesys);
    virtual ~CellStoreV0();

    virtual void create(const char *fname, size_t max_entries, PropertiesPtr &);
    virtual void add(const Key &key, const ByteString value);
    virtual void finalize(TableIdentifier *table_identifier);
    virtual void open(const String &fname, const String &start_row,
                      const String &end_row, int32_t fd, int64_t file_length,
                      CellStoreTrailer *trailer);
    virtual int64_t get_blocksize() { return m_trailer.blocksize; }
    virtual bool may_contain(const void *ptr, size_t len);
    bool may_contain(const String &key) {
      return may_contain(key.data(), key.size());
    }
    virtual bool may_contain(ScanContextPtr &);

    virtual int64_t get_revision();
    virtual uint64_t disk_usage() { return m_disk_usage; }
    virtual float compression_ratio() { return m_trailer.compression_ratio; }
    virtual const char *get_split_row();
    virtual int64_t get_total_entries() { return m_trailer.total_entries; }
    virtual std::string &get_filename() { return m_filename; }
    virtual int get_file_id() { return m_file_id; }
    virtual int64_t get_data_end() { return m_trailer.fix_index_offset; }
    virtual CellListScanner *create_scanner(ScanContextPtr &scan_ctx);
    virtual BlockCompressionCodec *create_block_compression_codec();
    virtual void display_block_info();
    virtual BloomFilter *get_bloom_filter() { return m_bloom_filter; }
    virtual int64_t bloom_filter_memory_used() { return 0; }
    virtual int64_t block_index_memory_used() { return 0; }
    virtual void maybe_purge_indexes(uint64_t access_counter) { return; }
    virtual int64_t purgeable_index_memory(uint64_t access_counter) { return false; }
    virtual bool restricted_range() { return true; }

    virtual int32_t get_fd() {
      ScopedLock lock(m_mutex);
      return m_fd;
    }

    virtual int32_t reopen_fd() {
      ScopedLock lock(m_mutex);
      if (m_fd != -1)
        m_filesys->close(m_fd);
      m_fd = m_filesys->open(m_filename);
      return m_fd;
    }

    virtual CellStoreTrailer *get_trailer() { return &m_trailer; }

  protected:
    void add_index_entry(const SerializedKey key, uint32_t offset);
    void record_split_row(const SerializedKey key);
    void create_bloom_filter(bool is_approx = false);
    void load_index();

    typedef std::map<SerializedKey, uint32_t> IndexMap;
    typedef BlobHashSet<> BloomFilterItems;

    Mutex                  m_mutex;
    Filesystem            *m_filesys;
    int32_t                m_fd;
    std::string            m_filename;
    IndexMap               m_index;
    CellStoreBlockIndexMap<uint32_t> m_index_map32;
    CellStoreTrailerV0     m_trailer;
    BlockCompressionCodec *m_compressor;
    DynamicBuffer          m_buffer;
    DynamicBuffer          m_fix_index_buffer;
    DynamicBuffer          m_var_index_buffer;
    uint32_t               m_memory_consumed;
    DispatchHandlerSynchronizer  m_sync_handler;
    uint32_t               m_outstanding_appends;
    uint32_t               m_offset;
    ByteString             m_last_key;
    int64_t                m_file_length;
    int64_t                m_disk_usage;
    std::string            m_split_row;
    int                    m_file_id;
    float                  m_uncompressed_data;
    float                  m_compressed_data;
    uint32_t               m_uncompressed_blocksize;
    BlockCompressionCodec::Args m_compressor_args;
    size_t                 m_max_entries;

    BloomFilterMode        m_bloom_filter_mode;
    BloomFilter           *m_bloom_filter;
    BloomFilterItems      *m_bloom_filter_items;
    uint32_t               m_max_approx_items;
  };

  typedef intrusive_ptr<CellStoreV0> CellStoreV0Ptr;

} // namespace Hypertable

#endif // HYPERTABLE_CELLSTOREV0_H
