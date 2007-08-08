/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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

#include <boost/shared_ptr.hpp>

#include "Key.h"
#include "CellList.h"
#include "Constants.h"


namespace hypertable {

  class CellStore : public CellList {
  public:

    static const uint16_t FLAG_SHARED;

    virtual ~CellStore() { return; }
    virtual int Create(const char *fname, size_t blockSize=Constants::DEFAULT_BLOCKSIZE) = 0;
    virtual int Finalize(uint64_t timestamp) = 0;
    virtual int Open(const char *fname, const KeyT *startKey, const KeyT *endKey) = 0;
    virtual int LoadIndex() = 0;
    virtual uint64_t GetLogCutoffTime() = 0;
    virtual uint64_t DiskUsage() = 0;
    virtual std::string &GetFilename() = 0;
    virtual uint16_t GetFlags() = 0;
    virtual KeyT *GetSplitKey() = 0;

  };

  typedef boost::shared_ptr<CellStore> CellStorePtr;

}

#endif // HYPERTABLE_CELLSTORE_H
