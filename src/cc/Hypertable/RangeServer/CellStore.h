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
#ifndef HYPERTABLE_CELLSTORE_H
#define HYPERTABLE_CELLSTORE_H

#include <boost/intrusive_ptr.hpp>

#include "Common/ByteString.h"

#include "CellList.h"
#include "CellStoreTrailer.h"
#include "Timestamp.h"

namespace Hypertable {

  /**
   * Abstract base class for persistent cell lists (ones that are stored on disk).
   */
  class CellStore : public CellList {
  public:

    virtual ~CellStore() { return; }
    
    virtual int add(const ByteString32T *key, const ByteString32T *value, uint64_t real_timestamp) = 0;

    virtual const char *get_split_row() = 0;

    virtual CellListScanner *create_scanner(ScanContextPtr &scanContextPtr) { return 0; }

    /**
     * Creates a new cell store.
     *
     * @param fname name of file to contain the cell store
     * @param blockSize amount of uncompressed data to compress into a block
     * @param compressor string indicating compressor type and arguments (e.g. "zlib --best")
     * @return Error::OK on success, error code on failure
     */
    virtual int create(const char *fname, uint32_t blocksize, const std::string &compressor) = 0;
    
    /**
     * Finalizes the creation of a cell store, by writing block index and metadata trailer.
     *
     * @param timestamp timestamp (both real and logical) of most recent update in the store
     * @return Error::OK on success, error code on failure
     */
    virtual int finalize(Timestamp &timestamp) = 0;
    
    /**
     * Opens a cell store with possibly a restricted view.  When a range splits, the cell stores
     * that comprise the range get shared between the two newly created ranges.  This method allows
     * each of the two ranges to open the same cell store but view different portions of it.
     *
     * @param fname pathname of file containing cell store
     * @param start_row restricts view of this store to key/value pairs that are greater than this value
     * @param end_row restricts view of this store to key/value pairs that are less than or equal to this value
     * @return Error::OK on success, error code on failure
     */
    virtual int open(const char *fname, const char *start_row, const char *end_row) = 0;

    /**
     * Loads the block index data into an in-memory map.
     * 
     * @return Error::OK on success, error code on failure
     */
    virtual int load_index() = 0;

    /**
     * Returns the block size used for this cell store.  The block size is the amount of
     * uncompressed key/value pairs to collect before compressing and storing as a 
     * compressed block in the cell store.
     *
     * @return block size
     */
    virtual uint32_t get_blocksize() = 0;

    /**
     * Returns the timestamp of the latest (newest) key/value pair in this cell store.
     * All key timestamps in this cell store are less than or equal to this value.
     *
     * @param timestamp reference to return object to hold timestamp(s)
     */
    virtual void get_timestamp(Timestamp &timestamp) = 0;

    /**
     * Returns the disk used by this cell store.  If the cell store is opened with
     * a restricted range, then it returns an estimate of the disk used by that range.
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
     * Return a pointer to the trailer object for this cell store
     *
     * @return pointer to the internal trailer of this cell store
     */
    virtual CellStoreTrailer *get_trailer() = 0;

  };

  typedef boost::intrusive_ptr<CellStore> CellStorePtr;

}

#endif // HYPERTABLE_CELLSTORE_H
