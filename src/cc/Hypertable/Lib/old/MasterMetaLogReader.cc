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
using namespace Hypertable::OldMetaLog;
using namespace Hypertable::OldMetaLog::MetaLogEntryFactory;
using namespace Serialization;
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


void load_entry(Reader &rd, SsiSet &ssi_set, RangeMoveStarted *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);

  if (it == ssi_set.end()) {
    HT_ERROR_OUT <<"Unexpected RangeMoveStarted"<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.push_back(ep);
}

void load_entry(Reader &rd, SsiSet &ssi_set, RangeMoveRestarted *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);

  if (it == ssi_set.end()) {
    HT_ERROR_OUT <<"Unexpected RangeMoveRestarted"<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.push_back(ep);
}

void load_entry(Reader &rd, SsiSet &ssi_set, RangeMoveLoaded *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);
  RangeMoveStarted *started;
  RangeMoveAcknowledged *acknowledged;
  bool found_started = false;
  bool found_acknowledged = false;

  if (it != ssi_set.end() && !(*it)->transactions.empty()) {
    for (std::list<MetaLogEntryPtr>::iterator txiter = (*it)->transactions.begin();
         txiter != (*it)->transactions.end(); ++txiter) {
      if ((*txiter)->get_type() == MASTER_RANGE_MOVE_STARTED) {
        started = dynamic_cast<RangeMoveStarted *>((*txiter).get());
        if (started->table == ep->table && started->range == ep->range)
          found_started = true;
      }
      else if ((*txiter)->get_type() == MASTER_RANGE_MOVE_ACKNOWLEDGED) {
        acknowledged = dynamic_cast<RangeMoveAcknowledged*>((*txiter).get());
        if (acknowledged->table == ep->table && acknowledged->range == ep->range)
          found_acknowledged = true;
      }
    }
  }
  // store move loaded txn
  if (found_started) {
    if (!found_acknowledged)
      (*it)->transactions.push_back(ep);
    else {
      // range loaded and acknowledged, purge txns
      std::list<MetaLogEntryPtr>::iterator deliter;
      for (std::list<MetaLogEntryPtr>::iterator txiter = (*it)->transactions.begin();
          txiter != (*it)->transactions.end();) {
        if ((*txiter)->get_type() == MASTER_RANGE_MOVE_STARTED) {
          started = dynamic_cast<RangeMoveStarted *>((*txiter).get());
          deliter = txiter;
          ++txiter;
          if (started->table == ep->table && started->range == ep->range) {
            // drop these entries since the range was successfully loaded and acknowledged
            (*it)->transactions.erase(deliter);
          }
        }
        else if ((*txiter)->get_type() == MASTER_RANGE_MOVE_ACKNOWLEDGED) {
          acknowledged = dynamic_cast<RangeMoveAcknowledged*>((*txiter).get());
          deliter = txiter;
          ++txiter;
          if (acknowledged->table == ep->table && acknowledged->range == ep->range) {
            // drop these entries because range was sucessfully loaded and acknowledged
            (*it)->transactions.erase(deliter);
          }
        }
        else
          ++txiter;
      }
    }
  }
  else {
    HT_ERROR_OUT <<"No matching RangeMoveStarted for RangeMoveLoaded "<< ep
                 << " at "<< rd.pos() <<'/' << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
}

void load_entry(Reader &rd, SsiSet &ssi_set, RangeMoveAcknowledged *ep) {
  ServerStateInfo si(ep->location);
  SsiSet::iterator it = ssi_set.find(&si);
  RangeMoveStarted *started;
  RangeMoveLoaded *loaded;
  bool found_started, found_loaded;
  found_started = found_loaded = false;

  if (it != ssi_set.end() && !(*it)->transactions.empty()) {
    for (std::list<MetaLogEntryPtr>::iterator txiter = (*it)->transactions.begin();
         txiter != (*it)->transactions.end(); ++txiter) {
      if ((*txiter)->get_type() == MASTER_RANGE_MOVE_STARTED) {
        started = dynamic_cast<RangeMoveStarted*>((*txiter).get());
        if (started->table == ep->table && started->range == ep->range)
          found_started = true;
      }
      else if ((*txiter)->get_type() == MASTER_RANGE_MOVE_LOADED) {
        loaded = dynamic_cast<RangeMoveLoaded*>((*txiter).get());
        if (loaded->table == ep->table && loaded->range == ep->range)
          found_loaded = true;
      }
    }
  }

  if (found_started) {
    if (!found_loaded)
      (*it)->transactions.push_back(ep);
    else {
      // range loaded and acknowledged, purge txns
      std::list<MetaLogEntryPtr>::iterator deliter;
      for (std::list<MetaLogEntryPtr>::iterator txiter = (*it)->transactions.begin();
          txiter != (*it)->transactions.end();) {
        if ((*txiter)->get_type() == MASTER_RANGE_MOVE_STARTED) {
          started = dynamic_cast<RangeMoveStarted *>((*txiter).get());
          deliter = txiter;
          ++txiter;
          // drop these entries since the range was successfully loaded and acknowledged
          if (started->table == ep->table && started->range == ep->range)
            (*it)->transactions.erase(deliter);
        }
        else if ((*txiter)->get_type() == MASTER_RANGE_MOVE_LOADED) {
          loaded = dynamic_cast<RangeMoveLoaded*>((*txiter).get());
          deliter = txiter;
          ++txiter;
          // drop these entries because range was sucessfully loaded and acknowledged
          if (loaded->table == ep->table && loaded->range == ep->range)
            (*it)->transactions.erase(deliter);
        }
        else
          ++txiter;
      }
    }
    return;
  }
  else {
    HT_ERROR_OUT << "No matching RangeMoveStarted for RangeMoveAcknowledged"<< ep
                   << " at "<< rd.pos() <<'/' << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
}

void load_entry(Reader &rd, SsiSet &ssi_set, MmlRecover *ep) {
  return;
}

void load_entry(Reader &rd, SsiSet &ssi_set, BalanceStarted *ep) {
  rd.set_balance_started(ep);
  return;
}
void load_entry(Reader &rd, SsiSet &ssi_set, BalanceDone *ep) {
  if (!(rd.get_balance_started())) {
    HT_ERROR_OUT << "No matching BalanceStarted for BalanceDone"<< ep
                << " at "<< rd.pos() <<'/' << rd.size() <<" in "<< rd.path() << HT_END;
    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  rd.set_balance_started((MetaLogEntry *)0);
  return;
}




} // local namespace

namespace Hypertable {

  namespace OldMetaLog {

MasterMetaLogReader::MasterMetaLogReader(Filesystem *fs, const String &path)
    : Parent(fs, path), m_recover(false), m_balance_started(0) {
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
    case MASTER_RANGE_MOVE_STARTED:
      load_entry(*this, ssi_set, (RangeMoveStarted*)p);       break;
    case MASTER_RANGE_MOVE_RESTARTED:
      load_entry(*this, ssi_set, (RangeMoveRestarted*)p);       break;
    case MASTER_RANGE_MOVE_LOADED:
      load_entry(*this, ssi_set, (RangeMoveLoaded *)p);         break;
    case MASTER_RANGE_MOVE_ACKNOWLEDGED:
      load_entry(*this, ssi_set, (RangeMoveAcknowledged*)p);         break;
    case MASTER_LOG_RECOVER:
      load_entry(*this, ssi_set, (MmlRecover *)p);
      m_recover = true;                                     break;
     case MASTER_BALANCE_STARTED:
      load_entry(*this, ssi_set, (BalanceStarted *)p);      break;
     case MASTER_BALANCE_DONE:
      load_entry(*this, ssi_set, (BalanceDone *)p);         break;
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

  }} // namespace Hypertable
