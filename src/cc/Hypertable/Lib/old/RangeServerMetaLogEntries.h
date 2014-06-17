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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_ENTRIES_H
#define HYPERTABLE_RANGE_SERVER_METALOG_ENTRIES_H

#include "MetaLog.h"
#include "MetaLogEntryBase.h"
#include "RangeServerMetaLogEntryFactory.h"

namespace Hypertable {

namespace OldMetaLog {

/**
 * Specific metalog entries for range server
 */
namespace RangeServerTxn {

class RangeLoaded : public MetaLogEntryRangeCommon {
public:
  RangeLoaded() {}
  RangeLoaded(const TableIdentifier &t, const RangeSpec &r, const RangeState &s)
    : MetaLogEntryRangeCommon(t, r, s) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_RANGE_LOADED; }
};

class SplitStart : public MetaLogEntryRangeCommon {
public:
  SplitStart() {}
  SplitStart(const TableIdentifier &t, const RangeSpec &old_range,
             const RangeState &state)
    : MetaLogEntryRangeCommon(t, old_range, state) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_SPLIT_START; }
};

class SplitShrunk : public MetaLogEntryRangeCommon {
public:
  SplitShrunk() {}
  SplitShrunk(const TableIdentifier &t, const RangeSpec &r, const RangeState &s)
    : MetaLogEntryRangeCommon(t, r, s) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_SPLIT_SHRUNK; }
};

class SplitDone : public MetaLogEntryRangeCommon {
public:
  SplitDone() {}
  SplitDone(const TableIdentifier &t, const RangeSpec &r, const RangeState &s)
      : MetaLogEntryRangeCommon(t, r, s) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_SPLIT_DONE; }
};

class RelinquishStart : public MetaLogEntryRangeCommon {
public:
  RelinquishStart() {}
  RelinquishStart(const TableIdentifier &t, const RangeSpec &r,
                  const RangeState &s) : MetaLogEntryRangeCommon(t, r, s) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_RELINQUISH_START; }
};

class RelinquishDone : public MetaLogEntryRangeBase {
public:
  RelinquishDone() {}
  RelinquishDone(const TableIdentifier &t, const RangeSpec &r)
      : MetaLogEntryRangeBase(t, r) {}

  virtual int get_type() const { return MetaLogEntryFactory::RS_RELINQUISH_DONE; }
};

class DropTable : public MetaLogEntryRangeBase {
public:
  DropTable() {}
  DropTable(const TableIdentifier &t) {
    table = t;
  }

  virtual void write(DynamicBuffer &);
  virtual const uint8_t *read(StaticBuffer &);
  virtual int get_type() const { return MetaLogEntryFactory::RS_DROP_TABLE; }
};

class RsmlRecover : public MetaLogEntry {
public:
  virtual void write(DynamicBuffer &) { return; }
  virtual const uint8_t *read(StaticBuffer &) { return 0; }
  virtual int get_type() const { return MetaLogEntryFactory::RS_LOG_RECOVER; }
};


}}} // namespace Hypertable::RangeServerTxn

#endif // HYPERTABLE_RANGE_SERVER_METALOG_ENTRIES_H
