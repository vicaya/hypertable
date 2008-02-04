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

#ifndef HYPERTABLE_CELLSTOREV0_H
#define HYPERTABLE_CELLSTOREV0_H

#include <map>
#include <string>
#include <vector>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/Filesystem.h"

#include "CellStore.h"
#include "CellStoreTrailerV0.h"

using namespace Hypertable;

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

    virtual int create(const char *fname, uint32_t blocksize, const std::string &compressor);
    virtual int add(const ByteString32T *key, const ByteString32T *value, uint64_t real_timestamp);
    virtual int finalize(Timestamp &timestamp);
    virtual int open(const char *fname, const char *start_row, const char *end_row);
    virtual int load_index();
    virtual uint32_t get_blocksize() { return m_trailer.blocksize; }
    virtual void get_timestamp(Timestamp &timestamp);
    virtual uint64_t disk_usage() { return m_disk_usage; }
    virtual float compression_ratio() { return m_trailer.compression_ratio; }
    virtual const char *get_split_row();
    virtual std::string &get_filename() { return m_filename; }
    virtual CellListScanner *create_scanner(ScanContextPtr &scanContextPtr);

    BlockCompressionCodec *create_block_compression_codec();

    /**
     * Displays block map information to stdout
     */
    void display_block_info();

    friend class CellStoreScannerV0;

    virtual CellStoreTrailer *get_trailer() { return &m_trailer; }

  protected:

    void add_index_entry(const ByteString32T *key, uint32_t offset);
    void record_split_row(const ByteString32T *key);

    static const char DATA_BLOCK_MAGIC[10];
    static const char INDEX_FIXED_BLOCK_MAGIC[10];
    static const char INDEX_VARIABLE_BLOCK_MAGIC[10];

    typedef map<ByteString32T *, uint32_t, ltByteString32> IndexMapT;

    Filesystem            *m_filesys;
    std::string            m_filename;
    int32_t                m_fd;
    IndexMapT              m_index;
    CellStoreTrailerV0     m_trailer;
    BlockCompressionCodec *m_compressor;
    DynamicBuffer          m_buffer;
    DynamicBuffer          m_fix_index_buffer;
    DynamicBuffer          m_var_index_buffer;
    DispatchHandlerSynchronizer  m_sync_handler;
    uint32_t               m_outstanding_appends;
    uint32_t               m_offset;
    ByteString32T         *m_last_key;
    uint64_t               m_file_length;
    uint32_t               m_disk_usage;
    std::string            m_split_row;
    int                    m_file_id;
    float                  m_uncompressed_data;
    float                  m_compressed_data;
    uint32_t               m_uncompressed_blocksize;
    BlockCompressionCodec::Args m_compressor_args;
  };
  typedef boost::intrusive_ptr<CellStoreV0> CellStoreV0Ptr;


}

#endif // HYPERTABLE_CELLSTOREV0_H
