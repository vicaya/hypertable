/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
#include "Filesystem.h"
#include "MetaLogVersion.h"
#include "RangeServerMetaLogReader.h"
#include "RangeServerMetaLogEntries.h"

using namespace Hypertable;
using namespace Serialization;
using namespace MetaLogEntryFactory;
using namespace RangeServerTxn;
using namespace Error;

namespace {

struct RsiSetTraits {
  bool operator()(const RangeStateInfo *x, const RangeStateInfo *y) const {
    if (x->table.id < y->table.id)
      return true;

    if (x->table.id > y->table.id)
      return false;

    int cmpval = std::strcmp(x->range.end_row, y->range.end_row);

    if (cmpval < 0)
      return true;

    if (cmpval > 0)
      return false;

    return std::strcmp(x->range.start_row, y->range.start_row) < 0;
  }
};

typedef std::set<RangeStateInfo *, RsiSetTraits> RsiSet;
typedef std::pair<RsiSet::iterator, bool> RsiInsRes;
typedef RangeServerMetaLogReader Reader;

RangeSpec original_range(const MetaLogEntryRangeCommon &e) {
  HT_ASSERT(e.range_state.split_point);
  HT_ASSERT(e.range_state.old_boundary_row);

  if (strcmp(e.range_state.split_point,
             e.range_state.old_boundary_row) < 0)
    /* split off high */
    return RangeSpec(e.range.start_row, e.range_state.old_boundary_row);
  /* split off low */
  return RangeSpec(e.range_state.old_boundary_row, e.range.end_row);
}

void load_entry(Reader &rd, RsiSet &rsi_set, RangeLoaded *ep) {
  RangeStateInfo *rsi = new RangeStateInfo(ep->table, ep->range,
      ep->range_state, ep->timestamp);
  RsiInsRes res = rsi_set.insert(rsi);

  if (!res.second) {
    HT_ERROR_OUT <<"Duplicated RangeLoaded "<< ep <<" at "<< rd.pos() <<"/"
                 << rd.size() <<" in "<< rd.path() << HT_END;
    delete rsi;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
}

void load_entry(Reader &rd, RsiSet &rsi_set, SplitStart *ep) {
  RangeStateInfo ri(ep->table, ep->range);
  RsiSet::iterator it = rsi_set.find(&ri);

  if (it == rsi_set.end()) {
    HT_ERROR_OUT <<"Unexpected SplitStart "<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.push_back(ep);
  (*it)->range_state = ep->range_state;
}


void load_entry(Reader &rd, RsiSet &rsi_set, SplitShrunk *ep) {
  RangeStateInfo ri(ep->table, original_range(*ep));
  RsiSet::iterator it = rsi_set.find(&ri);

  if (it == rsi_set.end() ||
      (*it)->transactions.empty() ||
      (*it)->transactions.front()->get_type() != RS_SPLIT_START) {
    HT_ERROR_OUT <<"Unexpected SplitShrunk "<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<< rd.path() << HT_END;

    if (it != rsi_set.end())
      HT_DEBUG_OUT << *it << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  RangeStateInfo *rsi = (*it);
  rsi->range = ep->range;
  rsi->range_state = ep->range_state;
  rsi->transactions.push_back(ep);
  // Since we've shrunk, we need to delete the old one,
  // and insert the new shrunken one
  rsi_set.erase(it);
  rsi_set.insert(rsi);
}


void load_entry(Reader &rd, RsiSet &rsi_set, SplitDone *ep) {
  RangeStateInfo ri(ep->table, ep->range);
  RsiSet::iterator it = rsi_set.find(&ri);

  if (it == rsi_set.end() ||
      (*it)->transactions.empty() ||
      (*it)->transactions.front()->get_type() != RS_SPLIT_START) {
    HT_ERROR_OUT <<"Unexpected SplitDone "<< ep << " at "<< rd.pos() <<'/'
                 << rd.size() <<" in "<<  rd.path() << HT_END;

    if (it != rsi_set.end())
      HT_DEBUG_OUT << *it << HT_END;

    if (rd.skips_errors())
      return;

    HT_THROW_(Error::METALOG_ENTRY_BAD_ORDER);
  }
  (*it)->transactions.clear();
  (*it)->range_state.clear();
}


void load_entry(Reader &rd, RsiSet &rsi_set, MoveStart *ep) {
  // TODO
}

void load_entry(Reader &rd, RsiSet &rsi_set, MovePrepared *ep) {
  // TODO
}

void load_entry(Reader &rd, RsiSet &rsi_set, MoveDone *ep) {
  // TODO
}

void load_entry(Reader &rd, RsiSet &rsi_set, DropTable *ep) {
  RsiSet::iterator it = rsi_set.begin();
  RsiSet::iterator tmpit;

  while (it != rsi_set.end()) {
    if ((*it)->table.id == ep->table.id) {
      tmpit = it;
      ++it;
      rsi_set.erase(tmpit);
    }
    else
      ++it;
  }
}

} // local namespace

namespace Hypertable {

RangeServerMetaLogReader::RangeServerMetaLogReader(Filesystem *fs,
                                                   const String &path)
    : Parent(fs, path) {
  uint8_t buf[RSML_HEADER_SIZE];

  if (fd() == -1)
    return;

  size_t nread = fs->read(fd(), buf, RSML_HEADER_SIZE);

  HT_EXPECT(nread == RSML_HEADER_SIZE, Error::METALOG_BAD_RS_HEADER);

  MetaLogHeader header(buf, RSML_HEADER_SIZE);

  HT_EXPECT(!strcmp(header.prefix, RSML_PREFIX), Error::METALOG_BAD_RS_HEADER);
  HT_EXPECT(header.version == RSML_VERSION, Error::METALOG_VERSION_MISMATCH);

  pos() += nread;
}

RangeServerMetaLogReader::~RangeServerMetaLogReader() {
  foreach(RangeStateInfo *i, m_range_states) delete i;
}

MetaLogEntry *
RangeServerMetaLogReader::read() {
  ScanEntry entry;

  if (!next(entry))
    return 0;

  StaticBuffer buf(entry.buf);
  return new_from_payload(RangeServerMetaLogEntryType(entry.type),
                          entry.timestamp, buf);
}

const RangeStates &
RangeServerMetaLogReader::load_range_states(bool force) {
  if (!force && m_range_states.size())
    return m_range_states;      // already loaded

  if (pos() > RSML_HEADER_SIZE) {
    // need to start from scratch, as seek doesn't work on buffered reads yet
    RangeServerMetaLogReaderPtr p(new RangeServerMetaLogReader(&fs(), path()));
    p->load_range_states();
    p->m_log_entries.swap(m_log_entries);
    p->m_range_states.swap(m_range_states);
    return m_range_states;
  }
  RsiSet rsi_set;

  for (MetaLogEntry *p = read(); p; p = read()) {
    switch (p->get_type()) {
    case RS_RANGE_LOADED:
      load_entry(*this, rsi_set, (RangeLoaded *)p);     break;
    case RS_SPLIT_START:
      load_entry(*this, rsi_set, (SplitStart *)p);      break;
    case RS_SPLIT_DONE:
      load_entry(*this, rsi_set, (SplitDone *)p);       break;
    case RS_SPLIT_SHRUNK:
      load_entry(*this, rsi_set, (SplitShrunk *)p);     break;
    case RS_MOVE_START:
      load_entry(*this, rsi_set, (MoveStart *)p);       break;
    case RS_MOVE_PREPARED:
      load_entry(*this, rsi_set, (MovePrepared *)p);    break;
    case RS_MOVE_DONE:
      load_entry(*this, rsi_set, (MoveDone *)p);        break;
    case RS_DROP_TABLE:
      load_entry(*this, rsi_set, (DropTable *)p);       break;
    default:
      HT_FATALF("Bad code: unhandled entry type: %d", p->get_type());
    }
    m_log_entries.push_back(p);
  }
  std::copy(rsi_set.begin(), rsi_set.end(), back_inserter(m_range_states));

  return m_range_states;
}

std::ostream &operator<<(std::ostream &out, const RangeStateInfo &info) {
  out <<"{RangeStateInfo: table="<< info.table <<"\n  range="<< info.range;

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
