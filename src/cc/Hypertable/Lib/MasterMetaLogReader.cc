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

#include "Common/Compat.h"
#include <set>
#include "Common/Logger.h"
#include "Common/CstrHashMap.h"
#include "Common/Serialization.h"
#include "Common/CstrHashMap.h"
#include "Common/Filesystem.h"
#include "MetaLogVersion.h"
#include "MasterMetaLogReader.h"
#include "MasterMetaLogEntries.h"

using namespace Hypertable;
using namespace Serialization;
using namespace MetaLogEntryFactory;
using namespace MasterTxn;
using namespace Error;

namespace {

struct SsiSetTraits {
  bool operator()(const ServerStateInfo *x, const ServerStateInfo *y) const {
    return strcmp(x->location.c_str(), y->location.c_str()) < 0;
  }
};

typedef std::set<ServerStateInfo *, SsiSetTraits> SsiSet;
typedef std::pair<SsiSet::iterator, bool> SsiInsRes;
typedef MasterMetaLogReader Reader;

void load_entry(Reader &rd, SsiSet &ssi_set, ServerJoined *ep) {
  ServerStateInfo *ssi = new ServerStateInfo(ep->location, ep->timestamp);
  ssi->transactions.push_back(ep);
  SsiInsRes res = ssi_set.insert(ssi);

  if (!res.second) {
    HT_ERROR_OUT <<"Duplicated ServerJoined "<< ep <<" at "<< rd.pos() <<"/"
                 << rd.size() <<" in "<< rd.path() << HT_END;
    delete ssi;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
}

void load_entry(Reader &rd, SsiSet &ssi_set, ServerLeft *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);

  if (it == ssi_set.end()) {
    HT_ERROR_OUT <<"Unexpected ServerLeft "<< ep <<" at "<< rd.pos() <<"/"
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.push_back(ep);
}

void load_entry(Reader &rd, SsiSet &ssi_set, ServerRemoved *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);

  if (it == ssi_set.end()) {
    HT_ERROR_OUT <<"Unexpected ServerRemoved "<< ep <<" at "<< rd.pos() <<"/"
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }

  // remove from set
  delete *it;
  ssi_set.erase(it);
}


void load_entry(Reader &rd, SsiSet &ssi_set, RangeAssigned *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);

  if (it == ssi_set.end()) {
    HT_ERROR_OUT <<"Unexpected RangeAssigned "<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.push_back(ep);
}

void load_entry(Reader &rd, SsiSet &ssi_set, RangeLoaded *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);
  RangeAssigned *assigned;

  if (it != ssi_set.end() && !(*it)->transactions.empty()) {
    for (std::list<MetaLogEntryPtr>::iterator txiter = (*it)->transactions.begin();
         txiter != (*it)->transactions.end(); ++txiter) {
      if ((*txiter)->get_type() == MASTER_RANGE_ASSIGNED) {
        assigned = dynamic_cast<RangeAssigned *>((*txiter).get());
        if (assigned->table == ep->table && assigned->range == ep->range) {
          // drop these entries because range was sucessfully loaded
          (*it)->transactions.erase(txiter);
          return;
        }
      }
    }
  }

  HT_ERROR_OUT <<"No matching RangeAssigned for RangeLoaded "<< ep << " at "<< rd.pos() <<'/'
               << rd.size() <<" in "<< rd.path() << HT_END;

  if (rd.skips_errors())
    return;

  HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
}

void load_entry(Reader &rd, SsiSet &ssi_set, MmlRecover *ep) {
  return;
}


} // local namespace

namespace Hypertable {

MasterMetaLogReader::MasterMetaLogReader(Filesystem *fs, const String &path)
    : Parent(fs, path), m_recover(false) {
  uint8_t buf[MML_HEADER_SIZE];

  if (fd() == -1)
    return;

  size_t nread = fs->read(fd(), buf, MML_HEADER_SIZE);

  HT_EXPECT(nread == MML_HEADER_SIZE, Error::METALOG_BAD_RS_HEADER);

  MetaLogHeader header(buf, MML_HEADER_SIZE);

  HT_EXPECT(!strcmp(header.prefix, MML_PREFIX), Error::METALOG_BAD_RS_HEADER);
  HT_EXPECT(header.version == MML_VERSION, Error::METALOG_VERSION_MISMATCH);

  pos() += nread;
}

MasterMetaLogReader::~MasterMetaLogReader() {
  foreach(ServerStateInfo *i, m_server_states) delete i;
}

MetaLogEntry *
MasterMetaLogReader::read() {
  ScanEntry entry;

  if (!next(entry))
    return 0;

  StaticBuffer buf(entry.buf);
  return new_from_payload(MasterMetaLogEntryType(entry.type),
                          entry.timestamp, buf);
}

const ServerStates &
MasterMetaLogReader::load_server_states(bool *recover, bool force) {

  *recover = false;
  if (force)
    m_recover = false;
  else if (m_server_states.size()) {
    *recover = m_recover;
    return m_server_states;      // already loaded
  }

  if (pos() > MML_HEADER_SIZE) {
    // need to start from scratch, as seek doesn't work on buffered reads yet
    MasterMetaLogReaderPtr p(new MasterMetaLogReader(&fs(), path()));
    p->load_server_states(&m_recover);
    p->m_log_entries.swap(m_log_entries);
    p->m_server_states.swap(m_server_states);
    *recover = m_recover;
    return m_server_states;
  }
  SsiSet ssi_set;

  for (MetaLogEntry *p = read(); p; p = read()) {
    switch (p->get_type()) {
    case MASTER_SERVER_JOINED:
      load_entry(*this, ssi_set, (ServerJoined *)p);        break;
    case MASTER_SERVER_LEFT:
      load_entry(*this, ssi_set, (ServerLeft *)p);          break;
    case MASTER_SERVER_REMOVED:
      load_entry(*this, ssi_set, (ServerRemoved *)p);       break;
    case MASTER_RANGE_ASSIGNED:
      load_entry(*this, ssi_set, (RangeAssigned *)p);       break;
    case MASTER_RANGE_LOADED:
      load_entry(*this, ssi_set, (RangeLoaded *)p);         break;
    case MASTER_LOG_RECOVER:
      load_entry(*this, ssi_set, (MmlRecover *)p);
      m_recover = true;                                     break;
    default:
      HT_FATALF("Bad code: unhandled entry type: %d", p->get_type());
    }
    m_log_entries.push_back(p);
  }
  std::copy(ssi_set.begin(), ssi_set.end(), back_inserter(m_server_states));

  *recover = m_recover;
  return m_server_states;
}

std::ostream &operator<<(std::ostream &out, const ServerStateInfo &info) {
  out <<"{ServerStateInfo: location="<< info.location;

  if (info.transactions.size()) {
    out <<"\n  transactions=[\n";

    foreach(const MetaLogEntryPtr &ptr, info.transactions)
      out <<"    "<< ptr.get() <<'\n';

    out <<"  ]";
  }
  out <<"\n}\n";
  return out;
}

} // namespace Hypertable
