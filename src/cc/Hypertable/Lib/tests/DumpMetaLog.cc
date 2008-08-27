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
#include "Common/Logger.h"
#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/RangeServerMetaLog.h"
#include "Hypertable/Lib/RangeServerMetaLogReader.h"
#include "Hypertable/Lib/MasterMetaLogReader.h"

using namespace Hypertable;

namespace {

void dump_range_states(RangeServerMetaLogReader *rdr) {
  const RangeStates &rstates = rdr->load_range_states();

  foreach(const RangeStateInfo *i, rstates) std::cout << *i;
}

void dump_metalog(Filesystem &fs, const String &path, Config::VarMap &cfg) {
  try {
    // TODO: auto sensing master metalog
    RangeServerMetaLogReaderPtr rdr = new RangeServerMetaLogReader(&fs, path);

    if (cfg.count("states")) {
      dump_range_states(rdr.get());
      return;
    }

    MetaLogEntryPtr entry;

    if (cfg.count("copy")) {
      RangeServerMetaLogPtr log = new RangeServerMetaLog(&fs,
          cfg["copy"].as<String>());
      while ((entry = rdr->read()))
        log->write(entry.get());
    }
    else {
      while ((entry = rdr->read()))
        std::cout << entry.get() <<"\n";
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}

} // local namespace

int main(int ac, char *av[]) {
  try {
    Config::Desc desc("Usage: dump_metalog [options] <dfs-path>\nOptions");
    desc.add_options()
      ("dfs", Config::value<String>()->default_value("localhost:38030"),
          "Dfs broker location")
      ("copy", Config::value<String>(), "Copy metalog (until errors)")
      ("states,s", "Dump metalog as states tree")
      ;
    Config::Desc hidden;
    hidden.add_options()
      ("path", Config::value<String>(), "metalog path in dfs")
      ;
    Config::PositionalDesc p;
    p.add("path", -1);

    Config::init_with_comm(ac, av, &desc, &hidden, &p);

    if (Config::varmap.count("path") == 0) {
      std::cout << Config::description() << std::endl;
      return 1;
    }

    DfsBroker::Client *dfs = new DfsBroker::Client(
        Config::varmap["dfs"].as<String>().c_str(), 0,
        Config::varmap["timeout"].as<int>());

    dump_metalog(*dfs, Config::varmap["path"].as<String>(), Config::varmap);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
