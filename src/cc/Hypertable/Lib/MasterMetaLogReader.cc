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

#include "Common/Compat.h"
#include "MasterMetaLogReader.h"

namespace Hypertable {

MasterMetaLogReader::MasterMetaLogReader(Filesystem *fs, const String& path)
    : MetaLogReaderDfsBase(fs, path) {
  // TODO
}

MetaLogReader::ScanEntry *
MasterMetaLogReader::next(ScanEntry &) {
  // TODO
  return NULL;
}

MetaLogEntry *
MasterMetaLogReader::read() {
  // TODO
  return NULL;
}

const MasterStates &
MasterMetaLogReader::load_master_states(bool force) {
  // TODO
  return m_master_states;
}

std::ostream &operator<<(std::ostream &out, const MasterStateInfo &info) {
  out <<"{ range server to recover="<< info.range_server_to_recover;
  foreach(const MetaLogEntryPtr &ep, info.transactions) {
    out <<"  "<< ep.get() <<'\n';
  }
  out << "}\n";
  return out;
}

} // namespace Hypertable
