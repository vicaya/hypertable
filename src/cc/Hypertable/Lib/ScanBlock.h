/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_SCANBLOCK_H
#define HYPERTABLE_SCANBLOCK_H

#include <vector>

#include "AsyncComm/Event.h"
#include "Common/ByteString.h"

namespace Hypertable {

  /** Encapsulates a block of scan results.  The CREATE_SCANNER and
   * FETCH_SCANBLOCK RangeServer methods return a block of scan results
   * and this class parses and provides easy access to the key/value
   * pairs in that result.
   */
  class ScanBlock {
  public:

    typedef std::vector< std::pair<ByteString, ByteString> > Vector;

    ScanBlock();

    /** Loads scanblock data returned from RangeServer.  Both the CREATE_SCANNER and
     * FETCH_SCANBLOCK methods return a block of key/value pairs.
     *
     * @param event_ptr smart pointer to response MESSAGE event
     * @return Error::OK on success or error code on failure
     */
    int load(EventPtr &event_ptr);

    /** Returns the number of key/value pairs in the scanblock.
     *
     * @return number of key/value pairs in the scanblock
     */
    size_t size() { return m_vec.size(); }

    /** Resets iterator to first key/value pair in the scanblock. */
    void reset() { m_iter = m_vec.begin(); }

    /** Returns the next key/value pair in the scanblock.  <b>NOTE:</b> invoking
     * the #load method invalidates all pointers previously returned from this method.
     *
     * @param key reference to return key pointer
     * @param value reference to return value pointer
     * @return true if key/value returned, false if no more key/value pairs
     */
    bool next(ByteString &key, ByteString &value);

    /** Returns true if this is the final scanblock returned by the scanner.
     *
     * @return true if this is the final scanblock, or false if more to come
     */
    bool eos() { return ((m_flags & 0x0001) == 0x0001); }

    /** Indicates whether or not there are more key/value pairs in block
     *
     * @return ture if #next will return more key/value pairs, false otherwise
     */
    bool more() {
      if (m_iter == m_vec.end())
        return false;
      return true;
    }

    /** Returns scanner ID associated with this scanblock.
     *
     * @return scanner ID
     */
    int get_scanner_id() { return m_scanner_id; }

  private:
    int m_error;
    uint16_t m_flags;
    int m_scanner_id;
    Vector m_vec;
    Vector::iterator m_iter;
    EventPtr m_event_ptr;
  };
}

#endif // HYPERTABLE_SCANBLOCK_H
