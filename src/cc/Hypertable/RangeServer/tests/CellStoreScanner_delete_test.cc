/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/Config.h"
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

#include "../CellStoreV0.h"
#include "../FileBlockCache.h"
#include "../Global.h"

#include <cstdlib>

using namespace Hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_DFSBROKER_PORT = 38030;
  const char *usage[] = {
    "usage: CellStoreScanner_test",
    "",
    "  This program tests for the proper functioning of the CellStore",
    "  scanner.  It creates a dummy cell store and then repeatedly scans",
    "  it with different ranges",
    (const char *)0
  };
  const char *schema_str =
  "<Schema>\n"
  "  <AccessGroup name=\"default\">\n"
  "    <ColumnFamily id=\"1\">\n"
  "      <Name>tag</Name>\n"
  "    </ColumnFamily>\n"
  "  </AccessGroup>\n"
  "</Schema>";

  size_t display_scan(CellListScannerPtr &scanner, ostream &out) {
    Key key_comps;
    ByteString bsvalue;
    size_t count = 0;
    while (scanner->get(key_comps, bsvalue)) {
      out << key_comps << "\n";
      cout << key_comps << "\n";
      count++;
      scanner->forward();
    }
    out << flush;
    return count;
  }
}


int main(int argc, char **argv) {
  try {
    struct sockaddr_in addr;
    ConnectionManagerPtr conn_mgr;
    DfsBroker::ClientPtr client;
    CellStorePtr cs;
    std::ofstream out("CellStoreScanner_delete_test.output");
    String delete_row = "delete_row";
    String delete_cf  = "delete_cf";
    String delete_row_cf = "delete_row_cf";
    String delete_none = "delete_none";
    String insert = "insert";


    Config::init(argc, argv);

    if (Config::has("help"))
      Usage::dump_and_exit(usage);

    System::initialize(System::locate_install_dir(argv[0]));
    ReactorFactory::initialize(2);

    InetAddr::initialize(&addr, "localhost", DEFAULT_DFSBROKER_PORT);

    conn_mgr = new ConnectionManager();
    Global::dfs = new DfsBroker::Client(conn_mgr, addr, 15000);

    // force broker client to be destroyed before connection manager
    client = (DfsBroker::Client *)Global::dfs;

    if (!client->wait_for_connection(15000)) {
      HT_ERROR("Unable to connect to DFS");
      return 1;
    }

    Global::block_cache = new FileBlockCache(10000LL);

    String testdir = "/CellStoreScanner_delete_test";
    client->mkdirs(testdir);

    String csname = testdir + "/cs0";
    PropertiesPtr cs_props = new Properties();
    // make sure blocks are small so only one key value pair fits in a block
    cs_props->set("blocksize", uint32_t(32));
    cs = new CellStoreV0(Global::dfs);
    HT_TRY("creating cellstore", cs->create(csname.c_str(), 0, cs_props));

    DynamicBuffer dbuf(64000);
    String row;
    String qualifier;
    SerializedKey serkey;
    int64_t timestamp = 1;
    std::vector<SerializedKey> serkeyv;
    std::vector<Key> keyv;
    Key key;
    ScanContextPtr scan_ctx;
    String value="0";
    uint8_t valuebuf[128];
    uint8_t *uptr;
    ByteString bsvalue;

    uptr = valuebuf;
    Serialization::encode_vi32(&uptr,value.length());
    strcpy((char *)uptr, value.c_str());
    bsvalue.ptr = valuebuf;


    // test delete logic
    { 
      // delete row
      serkey.ptr = dbuf.ptr;
      row = delete_row;
      qualifier = insert;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, 
                            qualifier.c_str(), timestamp, timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete column family
      serkey.ptr = dbuf.ptr;
      row = delete_cf;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete row & column family
      serkey.ptr = dbuf.ptr;
      row = delete_row_cf;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_ROW, row.c_str(), 0, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      serkey.ptr = dbuf.ptr;
      create_key_and_append(dbuf, FLAG_DELETE_COLUMN_FAMILY, row.c_str(), 1, "", timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

      // delete none
      serkey.ptr = dbuf.ptr;
      row = delete_none;
      create_key_and_append(dbuf, FLAG_INSERT, row.c_str(), 1, qualifier.c_str(), timestamp,
                            timestamp);
      timestamp++;
      serkeyv.push_back(serkey);

    }

    sort(serkeyv.begin(), serkeyv.end());

    keyv.reserve( serkeyv.size() );

    out << "[baseline]\n";
    for (size_t i=0; i<serkeyv.size(); i++) {
      key.load( serkeyv[i] );
      cs->add(key, bsvalue);
      keyv.push_back(key);
      out << key << "\n";
    }

    TableIdentifier table_id;
    cs->finalize(&table_id);

    SchemaPtr schema = Schema::new_instance(schema_str, strlen(schema_str),
                                            true);
    if (!schema->is_valid()) {
      HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
      exit(1);
    }

    RangeSpec range;
    range.start_row = "";
    range.end_row = Key::END_ROW_MARKER;

    ScanSpecBuilder ssbuilder;
    String column;

    CellListScannerPtr scanner;
    
    /**
     * Test deletes
     */

    out << "[delete-row]\n";
    ssbuilder.clear();
    row = delete_row;
    column = (String)"tag:" + qualifier; 
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);
    
    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag:a", true,
        row.c_str(), "tag:z", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_row(row.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);
  
    out << "[delete-cf]\n";
    ssbuilder.clear();
    row = delete_cf;
    column = (String)"tag:" + qualifier; 
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);
    
    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag", true,
        row.c_str(), "tag", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    ssbuilder.clear();
    ssbuilder.add_row(row.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);


    out << "[delete-row-cf]\n";
    ssbuilder.clear();
    row = delete_row_cf;
    column = (String)"tag:" + qualifier; 
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);
    
    ssbuilder.clear();
    ssbuilder.add_cell_interval(row.c_str(),"tag", true,
        row.c_str(), "tag", true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << "[delete-none]\n";
    ssbuilder.clear();
    row = delete_none;
    column = (String) "tag:"+qualifier;
    ssbuilder.add_cell(row.c_str(), column.c_str());
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                                   schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

  
    out << "[delete-all]\n";
    ssbuilder.clear();
    ssbuilder.add_row_interval("", true, Key::END_ROW_MARKER, true);
    scan_ctx = new ScanContext(TIMESTAMP_MAX, &(ssbuilder.get()), &range,
                               schema);
    scanner = cs->create_scanner(scan_ctx);
    display_scan(scanner, out);

    out << flush;
    String cmd_str = "diff CellStoreScanner_delete_test.output "
                     "CellStoreScanner_delete_test.golden";
    if (system(cmd_str.c_str()) != 0)
      return 1;

    client->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  catch (...) {
    HT_ERROR_OUT << "unexpected exception caught" << HT_END;
    return 1;
  }
  return 0;
}

