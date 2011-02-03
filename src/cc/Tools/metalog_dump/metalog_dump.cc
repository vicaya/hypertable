/**
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Init.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "DfsBroker/Lib/Config.h"
#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/MetaLog.h"
#include "Hypertable/Lib/MetaLogReader.h"

#include "Hypertable/RangeServer/MetaLogDefinitionRangeServer.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc("Usage: %s [options] <log-dir>\n\n"
                   "  This program dumps a textual representation of\n"
                   "  the given metalog.  If the last path component\n"
                   "  is a number, then the it is assumed to be an\n"
                   "  individual log file, otherwise the path is assumed\n"
                   "  to be the metalog directory\n\nOptions")
        .add_options()
        ("all", "Display all entities in log (not just latest state)")
        ;
      cmdline_hidden_desc().add_options()("log-path", str(), "dfs log path");
      cmdline_positional_desc().add("log-path", -1);
    }
    static void init() {
      if (!has("log-path")) {
        HT_ERROR_OUT <<"log-path required\n"<< cmdline_desc() << HT_END;
        exit(1);
      }
    }
  };

  typedef Meta::list<DfsClientPolicy, DefaultCommPolicy, AppPolicy> Policies;

  void determine_log_type(FilesystemPtr &fs, String path, String &name, bool *is_file) {
    boost::trim_right_if(path, boost::is_any_of("/"));

    *is_file = false;

    const char *base, *ptr;
    if ((base = strrchr(path.c_str(), '/')) != (const char *)0) {
      for (ptr=base+1; *ptr && isdigit(*ptr); ptr++)
        ;
      if (*ptr == 0)
        *is_file = true;
    }

    if (*is_file) {
      int fd = fs->open(path);
      MetaLog::Header header;
      uint8_t buf[MetaLog::Header::LENGTH];
      const uint8_t *ptr = buf;
      size_t remaining = MetaLog::Header::LENGTH;

      size_t nread = fs->read(fd, buf, MetaLog::Header::LENGTH);

      if (nread != MetaLog::Header::LENGTH)
        HT_THROWF(Error::METALOG_BAD_HEADER,
                  "Short read of header for '%s' (expected %d, got %d)",
                  path.c_str(), (int)MetaLog::Header::LENGTH, (int)nread);

      header.decode(&ptr, &remaining);
      name = header.name;
      fs->close(fd);
    }
    else
      name = String(base+1);
  }


} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);

    ConnectionManagerPtr conn_manager_ptr = new ConnectionManager();

    String log_path = get_str("log-path");
    String log_host = get("log-host", String());
    int timeout = has("dfs-timeout") ? get_i32("dfs-timeout") : 10000;
    bool dump_all = has("all");

    /**
     * Check for and connect to commit log DFS broker
     */
    DfsBroker::Client *dfs_client;

    if (log_host.length()) {
      int log_port = get_i16("log-port");
      InetAddr addr(log_host, log_port);

      dfs_client = new DfsBroker::Client(conn_manager_ptr, addr, timeout);
    }
    else {
      dfs_client = new DfsBroker::Client(conn_manager_ptr, properties);
    }

    if (!dfs_client->wait_for_connection(timeout)) {
      HT_ERROR("Unable to connect to DFS Broker, exiting...");
      exit(1);
    }

    // Population Defintion map
    hash_map<String, MetaLog::DefinitionPtr> defmap;
    MetaLog::DefinitionPtr def = new MetaLog::DefinitionRangeServer();
    defmap[def->name()] = def;

    FilesystemPtr fs = dfs_client;
    MetaLog::ReaderPtr rsml_reader;
    String name;
    bool is_file;

    determine_log_type(fs, log_path, name, &is_file);

    hash_map<String, MetaLog::DefinitionPtr>::iterator iter = defmap.find(name);
    if (iter == defmap.end()) {
      cerr << "No definition for log type '" << name << "'" << endl;
      exit(1);
    }
    def = iter->second;

    if (is_file) {
      rsml_reader = new MetaLog::Reader(fs, def);
      rsml_reader->load_file(log_path);
    }
    else
      rsml_reader = new MetaLog::Reader(fs, def, log_path);

    std::vector<MetaLog::EntityPtr> entities;

    if (dump_all)
      rsml_reader->get_all_entities(entities);
    else
      rsml_reader->get_entities(entities);

    for (size_t i=0; i<entities.size(); i++)
      cout << *entities[i] << "\n";
    cout << flush;

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
