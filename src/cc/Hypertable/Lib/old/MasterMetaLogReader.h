/** -*- c++ -*-
 * Copyright (C) 2008 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_MASTER_METALOG_READER_H
#define HYPERTABLE_MASTER_METALOG_READER_H

#include "Common/String.h"
#include "MetaLogReaderDfsBase.h"

#include <list>

namespace Hypertable {

  namespace OldMetaLog {

    struct ServerStateInfo {
      ServerStateInfo(const String &loc, uint64_t ts)
        : location(loc), timestamp(ts) {}

      ServerStateInfo(const String &loc)
        : location(loc) { }

      String location;
      int64_t timestamp; // time when the range is loaded
      std::list<MetaLogEntryPtr> transactions; // log entries associated with current txn
    };

    std::ostream &operator<<(std::ostream &, const ServerStateInfo &);

    typedef std::vector<ServerStateInfo *> ServerStates;

    class MasterMetaLogReader : public MetaLogReaderDfsBase {
    public:
      typedef MetaLogReaderDfsBase Parent;

      MasterMetaLogReader(Filesystem *, const String& path);
      virtual ~MasterMetaLogReader();

      virtual MetaLogEntry *read();

      const ServerStates &load_server_states(bool *recover, bool force = false);
      MetaLogEntryPtr &get_balance_started() {return m_balance_started;}
      void set_balance_started(MetaLogEntry *ep) {
        m_balance_started = ep;
      }

    private:
      ServerStates m_server_states;
      MetaLogEntries m_log_entries;
      bool m_recover;
      MetaLogEntryPtr m_balance_started;
    };

    typedef intrusive_ptr<MasterMetaLogReader> MasterMetaLogReaderPtr;

  } // namespace OldMetaLog

} // namespace Hypertable

#endif // HYPERTABLE_MASTER_METALOG_READER_H
