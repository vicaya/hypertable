/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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

#ifndef HYPERTABLE_MASTER_METALOG_ENTRIES_H
#define HYPERTABLE_MASTER_METALOG_ENTRIES_H

#include "MetaLog.h"
#include "MasterMetaLogEntryFactory.h"

namespace Hypertable {

  namespace OldMetaLog {

/**
 * Specific metalog entries for master
 */
namespace MasterTxn {

class ServerJoined : public MetaLogEntry {
public:
  ServerJoined() {}
  ServerJoined(const String &loc) : location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_SERVER_JOINED; }

  String location;
};

class ServerLeft : public MetaLogEntry {
public:
  ServerLeft() {}
  ServerLeft(const String &loc) : location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_SERVER_LEFT; }

  String location;
};

class ServerRemoved : public MetaLogEntry {
public:
  ServerRemoved() {}
  ServerRemoved(const String &loc) : location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_SERVER_REMOVED; }

  String location;
};

class RangeMoveStarted: public MetaLogEntry {
public:
  RangeMoveStarted() {}
  RangeMoveStarted(const TableIdentifier &tid, const RangeSpec &rspec,
                  const String &log, uint64_t sl, const String &loc)
    : table(tid), range(rspec), transfer_log(log), soft_limit(sl), location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_RANGE_MOVE_STARTED; }

  TableIdentifierManaged table;
  RangeSpecManaged range;
  String transfer_log;
  uint64_t soft_limit;
  String location;
};

class RangeMoveRestarted: public MetaLogEntry {
public:
  RangeMoveRestarted() {}
  RangeMoveRestarted(const TableIdentifier &tid, const RangeSpec &rspec,
                  const String &log, uint64_t sl, const String &loc)
    : table(tid), range(rspec), transfer_log(log), soft_limit(sl), location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_RANGE_MOVE_RESTARTED; }

  TableIdentifierManaged table;
  RangeSpecManaged range;
  String transfer_log;
  uint64_t soft_limit;
  String location;
};

class RangeMoveLoaded : public MetaLogEntry {
public:
  RangeMoveLoaded() {}
  RangeMoveLoaded(const TableIdentifier &tid, const RangeSpec &rspec,
                  const String &loc)
    : table(tid), range(rspec), location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_RANGE_MOVE_LOADED; }

  TableIdentifierManaged table;
  RangeSpecManaged range;
  String location;
};

class RangeMoveAcknowledged: public MetaLogEntry {
public:
  RangeMoveAcknowledged() {}
  RangeMoveAcknowledged(const TableIdentifier &tid, const RangeSpec &rspec,
                        const String &loc) : table(tid), range(rspec), location(loc) { }

  virtual void write(DynamicBuffer &buf);
  virtual const uint8_t *read(StaticBuffer &buf);
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_RANGE_MOVE_ACKNOWLEDGED; }

  TableIdentifierManaged table;
  RangeSpecManaged range;
  String location;
};

class MmlRecover : public MetaLogEntry {
public:
  virtual void write(DynamicBuffer &) { return; }
  virtual const uint8_t *read(StaticBuffer &) { return 0; }
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_LOG_RECOVER; }
};

class BalanceStarted : public MetaLogEntry {
public:
  virtual void write(DynamicBuffer &) { return; }
  virtual const uint8_t *read(StaticBuffer &) { return 0; }
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_BALANCE_STARTED; }
};

class BalanceDone: public MetaLogEntry {
public:
  virtual void write(DynamicBuffer &) { return; }
  virtual const uint8_t *read(StaticBuffer &) { return 0; }
  virtual int get_type() const { return MetaLogEntryFactory::MASTER_BALANCE_DONE; }
};



}}} // namespace Hypertable::MasterTxn

#endif // HYPERTABLE_MASTER_METALOG_ENTRIES_H
