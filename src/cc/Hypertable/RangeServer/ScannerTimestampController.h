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

#ifndef HYPERTABLE_SCANNERTIMESTAMPCONTROLLER_H
#define HYPERTABLE_SCANNERTIMESTAMPCONTROLLER_H

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/identity.hpp>
#include <boost/thread/mutex.hpp>

using namespace boost;
using namespace boost::multi_index;

namespace Hypertable {

  class ScannerTimestampController {

  public:

    void add_update_timestamp(uint64_t timestampp);
    void remove_update_timestamp(uint64_t timestamp);
    uint64_t get_oldest_update_timestamp();

  private:

    typedef multi_index_container<
      uint64_t,
      indexed_by<
      sequenced<>, // list-like index
      ordered_unique<identity<uint64_t> > // words by alphabetical order
      >
      > UpdateTimestampContainerT;

    boost::mutex m_mutex;
    UpdateTimestampContainerT m_update_timestamps;

  };

}

#endif // HYPERTABLE_SCANNERTIMESTAMPCONTROLLER_H

