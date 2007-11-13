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

#ifndef HYPERTABLE_CELLSTOREVERSION1_H
#define HYPERTABLE_CELLSTOREVERSION1_H

#include <map>
#include <string>
#include <vector>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "Hypertable/Lib/Filesystem.h"

#include "BlockDeflater.h"
#include "CellStore.h"

using namespace Hypertable;

/**
 * Forward declarations
 */
namespace Hypertable {
  class Client;
  class Protocol;
}

namespace Hypertable {

  class CellStoreV0 : public CellStore {

  public:

    CellStoreV0(Filesystem *filesys);
    virtual ~CellStoreV0();

    virtual int create(const char *fname, size_t blockSize=Constants::DEFAULT_BLOCKSIZE);
    virtual int add(const ByteString32T *key, const ByteString32T *value);
    virtual int finalize(uint64_t timestamp);

    virtual int open(const char *fname, const ByteString32T *startKey, const ByteString32T *endKey);
    virtual int load_index();
    virtual uint64_t get_log_cutoff_time();
    virtual uint64_t disk_usage() { return m_disk_usage; }
    virtual ByteString32T *get_split_key();
    virtual std::string &get_filename() { return m_filename; }
    virtual uint16_t get_flags();

    /**
     * Creates a CellStoreScannerV0 object that contains an shared pointer (intrusive_ptr)
     * to this CellStore;
     */
    virtual CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    friend class CellStoreScannerV0;

  protected:

    void add_index_entry(const ByteString32T *key, uint32_t offset);
    void record_split_key(const uint8_t *keyBytes);

    typedef struct {
      uint32_t  fixIndexOffset;
      uint32_t  varIndexOffset;
      uint32_t  splitKeyOffset;
      uint32_t  indexEntries;
      uint64_t  timestamp;
      uint16_t  flags;
      uint16_t  compressionType;
      uint16_t  version;
    } __attribute__((packed)) StoreTrailerT;

    typedef map<ByteString32T *, uint32_t, ltByteString32> IndexMapT;

    Filesystem            *m_filesys;
    std::string            m_filename;
    int32_t                m_fd;
    IndexMapT              m_index;
    StoreTrailerT          m_trailer;
    BlockDeflater         *m_block_deflater;
    DynamicBuffer          m_buffer;
    DynamicBuffer          m_fix_index_buffer;
    DynamicBuffer          m_var_index_buffer;
    size_t                 m_block_size;
    DispatchHandlerSynchronizer  m_sync_handler;
    uint32_t               m_outstanding_appends;
    uint32_t               m_offset;
    ByteString32T         *m_last_key;
    uint64_t               m_file_length;
    uint32_t               m_disk_usage;
    ByteString32Ptr        m_split_key;
    int                    m_file_id;
    ByteString32Ptr        m_start_key_ptr;
    ByteString32Ptr        m_end_key_ptr;
  };

}

#endif // HYPERTABLE_CELLSTOREVERSION1_H
