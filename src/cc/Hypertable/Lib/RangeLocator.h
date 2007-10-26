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

#ifndef HYPERTABLE_RANGELOCATOR_H
#define HYPERTABLE_RANGELOCATOR_H

#include "LocationCache.h"

namespace Hyperspace {
  class Session;
}

namespace hypertable {

  class RangeLocator {

  public:
    RangeLocator(Hyperspace::Session *hyperspace);
    ~RangeLocator();
    int Find(uint32_t tableId, const char *rowKey, const char **serverIdPtr);
    void SetRootStale() { mRootStale=true; }

  private:

    int ReadRootLocation();

    Hyperspace::Session *mHyperspace;
    LocationCache        mCache;
    uint64_t             mRootFileHandle;
    HandleCallbackPtr    mRootHandlerPtr;
    bool                 mRootStale;
  };

}

#endif // HYPERTABLE_RANGELOCATOR_H
