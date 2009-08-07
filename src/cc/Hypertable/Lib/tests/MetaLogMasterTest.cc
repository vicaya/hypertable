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
#include "Common/Init.h"
#include <iostream>
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MasterMetaLog.h"
#include "Hypertable/Lib/MasterMetaLogEntryFactory.h"
#include "Hypertable/Lib/MasterMetaLogReader.h"

using namespace Hypertable;
using namespace MetaLogEntryFactory;
using namespace Config;

namespace {

struct MyPolicy : Config::Policy {
  static void init_options() {
    Config::cmdline_desc().add_options()
      ("save,s", "Don't delete generated the log files")
      ("no-diff,n", "Don't compare with golden files yet")
      ;
  }
};

typedef Meta::list<MyPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

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
  try {
    init_with_policies<Policies>(ac, av);
    //TODO
    //write_test();
    //read_test();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
