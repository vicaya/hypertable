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
#ifndef HYPERTABLE_CELLLISTSCANNER_H
#define HYPERTABLE_CELLLISTSCANNER_H

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"
#include "Common/ReferenceCount.h"

#include "ScanContext.h"

using namespace Hypertable;

namespace Hypertable {

  class CellListScanner : public ReferenceCount {
  public:
    CellListScanner(ScanContextPtr &scanContextPtr) : m_scan_context_ptr(scanContextPtr) { return; }
    virtual ~CellListScanner() { return; }
    virtual void forward() = 0;
    virtual bool get(ByteString32T **keyp, ByteString32T **valuep) = 0;

  protected:
    ScanContextPtr m_scan_context_ptr;
  };

  typedef boost::intrusive_ptr<CellListScanner> CellListScannerPtr;

}

#endif // HYPERTABLE_CELLLISTSCANNER_H

