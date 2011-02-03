/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/Init.h"
#include "Common/DynamicBuffer.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include <iostream>
#include <fstream>

#include "AsyncComm/ConnectionManager.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "../CellStoreFactory.h"
#include "../CellStoreV4.h"
#include "../FileBlockCache.h"
#include "../Global.h"

#include <cstdlib>

using namespace Hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_DFSBROKER_PORT = 38030;
  const char *schema_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily id=\"1\">\n"
  "      <Name>column</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
    "</Schema>";
  const char *usage[] = {
    "usage: CellStoreScanner_test",
    "",
    "  This program tests for the proper functioning of the CellStore",
    "  scanner.  It creates a dummy cell store and then repeatedly scans",
    "  it with different ranges",
    (const char *)0
  };
}


int main(int argc, char **argv) {
  try {
    struct sockaddr_in addr;
    ConnectionManagerPtr conn_mgr;
    DfsBroker::ClientPtr client;
    CellStorePtr cs;
    DynamicBuffer key_buf;
    char *ptr, *value_data;
    DynamicBuffer value_buf;
    ByteString value_bs;
    Key key;
    char key_data[32];
    String install_dir, output_file;

    Config::init(argc, argv);

    if (Config::has("help"))
      Usage::dump_and_exit(usage);

    install_dir = System::install_dir;
    output_file = install_dir + "/CellStore64_test.output";

    ReactorFactory::initialize(2);

    InetAddr::initialize(&addr, "localhost", DEFAULT_DFSBROKER_PORT);

    conn_mgr = new ConnectionManager();
    Global::dfs = new DfsBroker::Client(conn_mgr, addr, 15000);

    // force broker client to be destroyed before connection manager
    client = (DfsBroker::Client *)Global::dfs.get();

    if (!client->wait_for_connection(15000)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }

    Global::block_cache = new FileBlockCache(10000000LL, 20000000LL);
    Global::memory_tracker = new MemoryTracker(Global::block_cache);

    String testdir = "/test/CellStore";
    String csname = testdir + "/cs64";

    client->mkdirs(testdir);

    Config::properties->set("Hypertable.RangeServer.CellStore.DefaultCompressor", String("none"));
    Config::properties->set("Hypertable.RangeServer.CellStore.DefaultBlockSize", 4*1024*1024);

    cs = new CellStoreV4(Global::dfs.get());
    HT_TRY("creating cellstore", cs->create(csname.c_str(), 4096, Config::properties));

    // setup value
    value_data = new char [ (1024*1024)+1 ];
    ptr = value_data;
    for (size_t i=0; i<131072; ++i) {
      memcpy(ptr, "testing ", 8);
      ptr += 8;
    }
    append_as_byte_string(value_buf, value_data, ptr-value_data);
    delete [] value_data;
    value_bs.ptr = value_buf.base;

    // setup key
    memset(&key, 0, sizeof(key));
    key.column_family_code = 1;
    key.flag = FLAG_INSERT;
    key.row = key_data;

    size_t keyi;
    for (keyi=0; keyi<4200; ++keyi) {
      sprintf(key_data, "%010u", (unsigned)keyi);
      key_buf.clear();
      create_key_and_append(key_buf, FLAG_INSERT, key_data, 1, 0);
      key.serial.ptr = key_buf.base;
      key.length = key_buf.fill();
      cs->add(key, value_bs);
    }
    TableIdentifier table_id;
    memset(&table_id, 0, sizeof(table_id));
    cs->finalize(&table_id);

    //cs = CellStoreFactory::open(csname, "", Key::END_ROW_MARKER);

    String cmd_str = install_dir + "/csdump /test/CellStore/cs64 | grep -v create_time > "
      + output_file;
    if (system(cmd_str.c_str()) != 0)
      return 1;

    std::ofstream out(output_file.c_str(), ios_base::out|ios_base::app);

    SchemaPtr schema = Schema::new_instance(schema_str, strlen(schema_str), true);
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }

    RangeSpec range_spec;
    range_spec.start_row = "";
    range_spec.end_row = Key::END_ROW_MARKER;

    ScanSpecBuilder ssbuilder;
    ByteString value;
    Key key_comps;
    ScanContextPtr scan_context;
    CellListScannerPtr scanner;

    ssbuilder.add_row("0000004135");

    scan_context = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range_spec, schema);
    scanner = cs->create_scanner(scan_context);
    while (scanner->get(key_comps, value)) {
      out << key_comps << endl;
      scanner->forward();
    }

    ssbuilder.clear();
    ssbuilder.add_row("0000004136");

    scan_context = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range_spec, schema);
    scanner = cs->create_scanner(scan_context);
    while (scanner->get(key_comps, value)) {
      out << key_comps << endl;
      scanner->forward();
    }

    ssbuilder.clear();
    ssbuilder.add_row_interval("0000004135", true, "0000004143", true);

    scan_context = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range_spec, schema);
    scanner = cs->create_scanner(scan_context);
    while (scanner->get(key_comps, value)) {
      out << key_comps << endl;
      scanner->forward();
    }

    ssbuilder.clear();
    ssbuilder.add_row_interval("0000004136", true, "0000004144", true);

    scan_context = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range_spec, schema);
    scanner = cs->create_scanner(scan_context);
    while (scanner->get(key_comps, value)) {
      out << key_comps << endl;
      scanner->forward();
    }

    out.close();

    cmd_str = String("diff ") + install_dir + "/CellStore64_test.output "
      + install_dir + "/CellStore64_test.golden";
    if (system(cmd_str.c_str()) != 0)
      return 1;

    client->rmdir(testdir);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  return 0;
}

