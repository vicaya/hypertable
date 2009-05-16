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

#ifndef HYPERTABLE_CELLSTORE_H
#define HYPERTABLE_CELLSTORE_H

#include "Common/ByteString.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Types.h"

#include "CellList.h"
#include "CellStoreTrailer.h"

namespace Hypertable {

  /**
   * Abstract base class for persistent cell lists (ones that are stored on
   * disk).
   */
  class CellStore : public CellList {
  public:
    virtual ~CellStore() { return; }

    virtual void add(const Key &key, const ByteString value) = 0;

    virtual const char *get_split_row() = 0;

    virtual uint32_t get_total_entries() = 0;

    virtual CellListScanner *
    create_scanner(ScanContextPtr &scan_ctx) { return 0; }

    /**
     * Creates a new cell store.
     *
     * @param fname name of file to contain the cell store
     * @param max_entries maximum number of entries the cell store is
     *        expected to have
     * @param props cellstore specific properties
     */
    virtual void create(const char *fname, size_t max_entries,
                        PropertiesPtr &props) = 0;

    /**
     * Finalizes the creation of a cell store, by writing block index and
     * metadata trailer.
     *
     * @param table_identifier table identifier
     */
    virtual void finalize(TableIdentifier *table_identifier) = 0;

    /**
     * Opens a cell store with possibly a restricted view.  When a range
     * splits, the cell stores that comprise the range get shared between the
     * two newly created ranges.  This method allows each of the two ranges to
     * open the same cell store but view different portions of it.
     *
     * @param fname pathname of file containing cell store
     * @param start_row restricts view of this store to key/value pairs that
     *        are greater than this value
     * @param end_row restricts view of this store to key/value pairs that are
     *        less than or equal to this value
     */
    virtual void open(const char *fname, const char *start_row,
                      const char *end_row) = 0;

    /**
     * Loads the block index data into an in-memory map.
     */
    virtual void load_index() = 0;

    /**
     * Returns the block size used for this cell store.  The block size is the
     * amount of uncompressed key/value pairs to collect before compressing and
     * storing as a compressed block in the cell store.
     *
     * @return block size
     */
    virtual uint32_t get_blocksize() = 0;

    /**
     * If the key is contained in this cell store, returns true.
     * If the key is not in the cell store, returns false with a high
     * probability (may return true - very low probability of this
     * happening)
     *
     * @param key key to query for - format is implementation dependent
     * @return true if cell store may contain the key
     */
    virtual bool may_contain(const void *key, size_t len) = 0;

    /**
     * This is a smarter variation that look at the scan context
     */
    virtual bool may_contain(ScanContextPtr &) = 0;

    /**
     * Returns the compaction revision of this cell store
     */
    virtual int64_t get_revision() = 0;

    /**
     * Returns the disk used by this cell store.  If the cell store is opened
     * with a restricted range, then it returns an estimate of the disk used by
     * that range.
     *
     * @return disk used by this cell store or portion thereof
     */
    virtual uint64_t disk_usage() = 0;

    /**
     * Returns block compression ratio of this cell store.
     *
     * @return compression ratio
     */
    virtual float compression_ratio() = 0;

    /**
     * Pathname of cell store file
     *
     * @return string reference to the cell store path name
     */
    virtual std::string &get_filename() = 0;

    /**
     * Returns a unique identifier which identifies the underlying file
     * for caching purposes
     *
     * @return unique file id
     */
    virtual int get_file_id() = 0;

    /**
     * Returns the offset of the end of the data portion of the file
     *
     * @return offset of end of data
     */
    virtual int64_t get_data_end() = 0;

    /**
     * Return a pointer to the trailer object for this cell store
     *
     * @return pointer to the internal trailer of this cell store
     */
    virtual CellStoreTrailer *get_trailer() = 0;

    /**
     * Creates a block compression codec suitable for decompressing
     * the cell store's blocks
     *
     * @return pointer to compression codec
     */
    virtual BlockCompressionCodec *create_block_compression_codec() = 0;

    /**
     * Returns the open file descriptor for the CellStore file
     *
     * @return open file descriptor
     */
    virtual int32_t get_fd() = 0;

    /**
     * Closes and reopens the underlying CellStore file
     *
     * @return new file descriptor
     */
    virtual int32_t reopen_fd() = 0;

    static const char DATA_BLOCK_MAGIC[10];
    static const char INDEX_FIXED_BLOCK_MAGIC[10];
    static const char INDEX_VARIABLE_BLOCK_MAGIC[10];

  };

  typedef intrusive_ptr<CellStore> CellStorePtr;

} // namespace Hypertable

#endif // HYPERTABLE_CELLSTORE_H
