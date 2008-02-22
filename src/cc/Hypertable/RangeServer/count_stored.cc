/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of
 * the License.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <fstream>
#include <iostream>
#include <string>

#include "Common/System.h"
#include "Common/Usage.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/KeySpec.h"

#include "CellStoreV0.h"
#include "CellStoreScannerV0.h"
#include "CellStoreTrailer.h"
#include "Global.h"


using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: count_stored <table>",
    "",
    "  This program counts the number of cells that exist in CellStores",
    "  for a table.  It does this by reading the Files columns for the",
    "  table in the METADATA table to learn of all the CellStores.  It",
    "  then does a linear scan of each CellStore file, counting the",
    "  number of cells...",
    "",
    (const char *)0
  };

  typedef struct {
    std::string start_row;
    std::string end_row;
    std::vector<std::string> cell_stores;
  } RangeCellStoreInfoT;

  typedef struct {
    std::string start_row;
    std::string end_row;
    std::string file;
  } CellStoreInfoT;
  
}

void fill_cell_store_vector(ClientPtr &hypertable_client_ptr, const char *table_name, std::vector<CellStoreInfoT> &file_vector);


int main(int argc, char **argv) {
  Comm *comm;
  ConnectionManagerPtr conn_manager_ptr;
  DfsBroker::Client *client;
  ClientPtr hypertable_client_ptr;
  std::vector<CellStoreInfoT> file_vector;
  const char *table_name = 0;
  bool hit_start = false;
  PropertiesPtr props_ptr;
  string config_file = "";
  CellStoreV0Ptr cell_store_ptr;
  ScanContextPtr scan_context_ptr( new ScanContext(ScanContext::END_OF_TIME) );
  ByteString32T *key;
  ByteString32T *value;
  uint64_t total_count = 0;
  uint64_t store_count = 0;

  for (int i=1; i<argc; i++) {
    if (argv[i][0] == '-')
      Usage::dump_and_exit(usage);
    else if (table_name == 0)
      table_name = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (table_name == 0)
    Usage::dump_and_exit(usage);

  try {
    // Create Hypertable client object
    hypertable_client_ptr = new Client(argv[0]);
  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  if (config_file == "")
    config_file = System::installDir + "/conf/hypertable.cfg";

  props_ptr = new Properties(config_file);

  comm = new Comm();
  conn_manager_ptr = new ConnectionManager(comm);

  client = new DfsBroker::Client(conn_manager_ptr, props_ptr);
  if (!client->wait_for_connection(15)) {
    cerr << "error: timed out waiting for DFS broker" << endl;
    exit(1);
  }

  Global::blockCache = new FileBlockCache(200000000LL);

  fill_cell_store_vector(hypertable_client_ptr, table_name, file_vector);

  for (size_t i=0; i<file_vector.size(); i++) {

    /**
     * Open cellStore
     */
    cell_store_ptr = new CellStoreV0(client);
    CellListScanner *scanner = 0;

    if (cell_store_ptr->open(file_vector[i].file.c_str(), 0, 0) != 0)
      return 1;

    if (cell_store_ptr->load_index() != 0)
      return 1;

    hit_start = (file_vector[i].start_row == "") ? true : false;

    store_count = 0;

    scanner = cell_store_ptr->create_scanner(scan_context_ptr);
    while (scanner->get(&key, &value)) {
      if (!hit_start) {
	if (strcmp((const char *)key->data, file_vector[i].start_row.c_str()) <= 0) {
	  scanner->forward();
	  continue;
	}
	hit_start = true;
      }
      if (strcmp((const char *)key->data, file_vector[i].end_row.c_str()) > 0)
	break;
      store_count++;
      scanner->forward();
    }
    delete scanner;

    cout << store_count << "\t" << file_vector[i].file << "[" << file_vector[i].start_row << ".." << file_vector[i].end_row << "]" << endl;
    total_count += store_count;

  }

  cout << total_count << "\tTOTAL" << endl;

  return 0;
}



/**
 *
 */
void fill_cell_store_vector(ClientPtr &hypertable_client_ptr, const char *table_name, std::vector<CellStoreInfoT> &file_vector) {
  int error;
  TablePtr table_ptr;
  TableScannerPtr scanner_ptr;
  ScanSpecificationT scan_spec;
  CellT cell;
  uint32_t table_id;
  char start_row[16];
  char end_row[16];
  RangeCellStoreInfoT range_cell_store_info;
  CellStoreInfoT cell_store_info;

  try {

    // Open the 'METADATA' table
    if ((error = hypertable_client_ptr->open_table("METADATA", table_ptr)) != Error::OK) {
      cerr << "Error: unable to open table 'WebServerLog' - " << Error::get_text(error) << endl;
      exit(1);
    }

    if ((error = hypertable_client_ptr->get_table_id(table_name, &table_id)) != Error::OK) {
      cerr << Error::get_text(error) << endl;
      exit(1);
    }

    // Set up the scan specification
    scan_spec.rowLimit = 0;
    scan_spec.max_versions = 1;
    sprintf(start_row, "%d:", table_id);
    scan_spec.startRow = start_row;
    sprintf(end_row, "%d:%s", table_id, Key::END_ROW_MARKER);
    scan_spec.endRow = end_row;
    scan_spec.startRowInclusive = scan_spec.endRowInclusive = true;
    scan_spec.columns.clear();
    scan_spec.columns.push_back("Files");
    scan_spec.columns.push_back("StartRow");
    scan_spec.interval.first = scan_spec.interval.second = 0;  // 0 means no time predicate
    scan_spec.return_deletes = false;

    // Create a scanner on the 'METADATA' table 
    if ((error = table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
      cerr << "Error: problem creating scanner on table 'WebServerLog' - " << Error::get_text(error) << endl;
      exit(1);
    }

  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    exit(1);
  }

  range_cell_store_info.start_row = "";
  range_cell_store_info.end_row = "";
  range_cell_store_info.cell_stores.clear();

  // Iterate through the cells returned by the scanner
  while (scanner_ptr->next(cell)) {
    if (strcmp(cell.row_key, range_cell_store_info.end_row.c_str())) {
      if (range_cell_store_info.end_row != "") {
	const char *end_row_cstr = strchr(range_cell_store_info.end_row.c_str(), ':');
	if (end_row_cstr == 0) {
	  cerr << "error: mal-formed end row (missing colon) - " << range_cell_store_info.end_row << endl;
	  exit(1);
	}
	end_row_cstr++;
	cell_store_info.start_row = range_cell_store_info.start_row;
	cell_store_info.end_row = end_row_cstr;
	for (size_t i=0; i<range_cell_store_info.cell_stores.size(); i++) {
	  cell_store_info.file = range_cell_store_info.cell_stores[i];
	  file_vector.push_back(cell_store_info);
	}
      }
      range_cell_store_info.start_row = "";
      range_cell_store_info.end_row = cell.row_key;
      range_cell_store_info.cell_stores.clear();      
    }

    if (!strcmp(cell.column_family, "StartRow"))
      range_cell_store_info.start_row = std::string((const char *)cell.value, cell.value_len);
    else if (!strcmp(cell.column_family, "Files")) {
      std::string files = std::string((const char *)cell.value, cell.value_len);
      char *ptr, *save_ptr;
      ptr = strtok_r((char *)files.c_str(), "\n\r;", &save_ptr);
      while (ptr) {
	range_cell_store_info.cell_stores.push_back(ptr);
	ptr = strtok_r(0, "\n\r;", &save_ptr);
      }
    }
    else {
      cerr << "Unexpected column family encountered: '" << cell.column_family << endl;
      exit(1);
    }
  }

  if (!range_cell_store_info.cell_stores.empty()) {
    const char *end_row_cstr = strchr(range_cell_store_info.end_row.c_str(), ':');
    if (end_row_cstr == 0) {
      cerr << "error: mal-formed end row (missing colon) - " << range_cell_store_info.end_row << endl;
      exit(1);
    }
    end_row_cstr++;
    cell_store_info.start_row = range_cell_store_info.start_row;
    cell_store_info.end_row = end_row_cstr;
    for (size_t i=0; i<range_cell_store_info.cell_stores.size(); i++) {
      cell_store_info.file = range_cell_store_info.cell_stores[i];
      file_vector.push_back(cell_store_info);
    }
  }
}
