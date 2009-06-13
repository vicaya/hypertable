/**
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

#include "Compat.h"
#include "FailureInducer.h"
#include "Error.h"
#include "Logger.h"

using namespace Hypertable;

namespace {
  enum { FAILURE_TYPE_EXIT, FAILURE_TYPE_THROW };
}

void FailureInducer::parse_option(String option) {
  char *istr = (char*)strchr(option.c_str(), ':');
  HT_ASSERT(istr != 0);
  *istr++ = 0;
  const char *failure_type = istr;
  istr = strchr(istr, ':');
  HT_ASSERT(istr != 0);
  *istr++ = 0;
  failure_inducer_state *statep = new failure_inducer_state;
  if (!strcmp(failure_type, "exit"))
    statep->failure_type = FAILURE_TYPE_EXIT;
  else if (!strcmp(failure_type, "throw"))
    statep->failure_type = FAILURE_TYPE_THROW;
  else
    HT_ASSERT(!"Unknown failure type");
  statep->iteration = 0;
  statep->trigger_iteration = atoi(istr);
  m_state_map[option.c_str()] = statep;
}

void FailureInducer::maybe_fail(const String &label) {
  ScopedLock lock(m_mutex);
  StateMap::iterator iter = m_state_map.find(label);
  if (iter != m_state_map.end()) {
    if ((*iter).second->iteration == (*iter).second->trigger_iteration) {
      if ((*iter).second->failure_type == FAILURE_TYPE_THROW) {
        uint32_t iteration = (*iter).second->iteration;
        delete (*iter).second;
        m_state_map.erase(iter);
        HT_THROW(Error::FAILED_EXPECTATION,
                 format("induced failure '%s' iteration=%u",
                        label.c_str(), iteration));
      }
      else {
        HT_ERRORF("induced failure '%s' iteration=%u",
                  (*iter).first.c_str(), (*iter).second->iteration);
        _exit(1);
      }
    }
    else
      (*iter).second->iteration++;
  }
}
