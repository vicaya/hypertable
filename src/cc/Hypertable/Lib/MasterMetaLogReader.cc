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

using namespace std;
using namespace Hypertable;

MasterMetaLogReader::MasterMetaLogReader(Filesystem *, const String& path) {
  // TODO
}

MetaLogReader::ScanEntry *
MasterMetaLogReader::next(ScanEntry *) {
  // TODO
  return NULL;
}

MasterMetaLogEntry *
MasterMetaLogReader::read() {
  // TODO
  return NULL;
}

void
MasterMetaLogReader::load_master_states(MasterStates &) {
  // TODO
}
