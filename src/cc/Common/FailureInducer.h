/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_FAILUREINDUCER_H
#define HYPERTABLE_FAILUREINDUCER_H

#include "HashMap.h"
#include "Mutex.h"
#include "String.h"
#include "StringExt.h"

namespace Hypertable {

  class FailureInducer {
  public:
    void parse_option(String option);
    void maybe_fail(const String &label);
  private:
    struct failure_inducer_state {
      uint32_t iteration;
      uint32_t trigger_iteration;
      int failure_type;
    };
    typedef hash_map<String, failure_inducer_state *> StateMap;
    Mutex m_mutex;
    StateMap m_state_map;
  };

}

#endif // HYPERTABLE_FAILUREINDUCER_H
