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

#ifndef HYPERTABLE_SCANBLOCK_H
#define HYPERTABLE_SCANBLOCK_H

#include <vector>

#include "AsyncComm/Event.h"
#include "Common/ByteString.h"

namespace hypertable {

  /** Encapsulates a block of scan results.  The CREATE_SCANNER and
   * FETCH_SCANBLOCK RangeServer methods return a block of scan results
   * and this class parses and provides easy access to the key/value
   * pairs in that result.
   */
  class ScanBlock {
  public:

    typedef std::vector< std::pair<const ByteString32T *, const ByteString32T *> > VectorT;

    /** Loads scanblock from event in response to CREATE_SCANNER or FETCH_SCANBLOCK RangeServer command.
     *
     * @param eventPtr smart pointer to response MESSAGE event
     * @return Error::OK on success or error code on failure
     */
    int load(EventPtr &eventPtr);

    /** Resets iterator to first result */
    void reset() { m_iter = m_vec.begin(); }

    /** Returns the next key/value pair in this result.  <b>NOTE:</b> invoking
     * the #load method invalidates all pointers previously returned from this method
     *
     * @param key reference to return key pointer
     * @param value reference to return value pointer
     * @return true if key/value returned, false if no more key/value pairs
     */
    bool next(const ByteString32T *&key, const ByteString32T *&value);

    /** Returns true if this is the final scanblock returned by the scanner.
     *
     * @return true if this is the final scanblock, or false if more to come
     */
    bool eos() { return ((m_flags & 0x0001) == 0x0001); }

    /** Returns the 32-bit scanner ID.
     *
     * @return scanner ID
     */
    int get_scanner_id() { return m_scanner_id; }
    
  private:
    int m_error;
    uint16_t m_flags;
    int m_scanner_id;
    VectorT m_vec;
    VectorT::iterator m_iter;
    EventPtr m_event_ptr;
  };
}

#endif // HYPERTABLE_SCANBLOCK_H
