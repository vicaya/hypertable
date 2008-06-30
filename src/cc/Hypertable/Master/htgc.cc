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
#include <iostream>
#include "Common/Thread.h"
#include "Common/System.h"
#include "Common/Logger.h"
#include "Common/Error.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "DfsBroker/Lib/Client.h"
#include "MasterGc.h"

using namespace Hypertable;

namespace {

void
do_tfgc(const String &config, bool dryrun) {
  PropertiesPtr props = new Properties(config);
  Comm *comm = new Comm();
  ConnectionManagerPtr conn_mgr = new ConnectionManager(comm);
  DfsBroker::Client *fs = new DfsBroker::Client(conn_mgr, props);
  ClientPtr client = new Hypertable::Client("htgc", config);
  TablePtr table = client->open_table("METADATA");
  master_gc_once(table, fs, dryrun);
}

} // local namespace

int
main(int ac, char *av[]) {
  try {
    Config::Desc desc("Usage: htgc [options]\nOptions");
    desc.add_options()
      ("dryrun,n", "Dryrun, don't modify (delete files etc.)")
      ;
    Config::init_with_comm(ac, av, &desc);
    do_tfgc(Config::cfgfile, Config::varmap.count("dryrun"));
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
