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

#ifndef HYPERTABLE_SCANRESULT_H
#define HYPERTABLE_SCANRESULT_H

#include <vector>

#include "AsyncComm/Event.h"
#include "Common/ByteString.h"

namespace hypertable {

  class ScanResult {
  public:

    typedef std::vector< std::pair<const ByteString32T *, const ByteString32T *> > VectorT;

    int load(EventPtr &eventPtr);
    VectorT &get_vector() { return m_vec; }
    bool eos() { return ((m_flags & 0x0001) == 0x0001); }
    int get_id() { return m_id; }
    
  private:
    int       m_error;
    uint16_t  m_flags;
    int       m_id;
    VectorT   m_vec;
    EventPtr  m_event_ptr;
  };
}

#endif // HYPERTABLE_SCANRESULT_H
