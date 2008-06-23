/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_MASTER_METALOG_READER_H
#define HYPERTABLE_MASTER_METALOG_READER_H

#include "Common/String.h"
#include "MetaLogReaderDfsBase.h"

namespace Hypertable {

struct MasterStateInfo {
  String range_server_to_recover;
  MetaLogEntries transactions;
};

std::ostream &operator<<(std::ostream &, const MasterStateInfo &);

typedef std::vector<MasterStateInfo *> MasterStates;

class MasterMetaLogReader : public MetaLogReaderDfsBase {
public:
  MasterMetaLogReader(Filesystem *, const String& path);

  virtual ScanEntry *next(ScanEntry &);
  virtual MetaLogEntry *read();

  const MasterStates &load_master_states(bool force = false);

private:
  MasterStates m_master_states;
};
typedef intrusive_ptr<MasterMetaLogReader> MasterMetaLogReaderPtr;

} // namespace Hypertable

#endif // HYPERTABLE_MASTER_METALOG_READER_H
