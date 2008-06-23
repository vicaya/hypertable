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
#include "Common/System.h"
#include <iostream>
#include "Hypertable/Lib/MasterMetaLog.h"
#include "Hypertable/Lib/MasterMetaLogEntryFactory.h"
#include "Hypertable/Lib/MasterMetaLogReader.h"

using namespace Hypertable;
using namespace MetaLogEntryFactory;

namespace {

void
write_test() {
  MasterMetaLogPtr metalog(new MasterMetaLog(NULL, "/test/master.log"));
  MetaLogEntryPtr log_entry(new_m_recovery_start("test_rs"));

  metalog->write(log_entry.get());
  metalog->close();
}

void
read_test() {
  MasterMetaLogReaderPtr reader(
      new MasterMetaLogReader(NULL, "/test/master.log"));
  MetaLogEntryPtr log_entry = reader->read();
  const MasterStates &mstates = reader->load_master_states();

  foreach(const MasterStateInfo *i, mstates)
    std::cout << *i << std::endl;
}

} // local namespace

int
main(int ac, char *av[]) {
  System::initialize(av[0]);
  //TODO
  //write_test();
  //read_test();
  return 0;
}
