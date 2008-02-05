/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <unistd.h>
#include "Common/Sweetener.h"
#include "Common/Thread.h"
#include "Common/Properties.h"
#include "Common/CharStrHashMap.h"
#include "Hypertable/Lib/Filesystem.h"
#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/Types.h"
#include "TableFileGc.h"

using namespace Hypertable;
using namespace std;

namespace {

typedef CharStrHashMap<int> CountMap;

struct GcWorker {
  GcWorker(TablePtr metadata, Filesystem *fs, int interval,
           bool dryrun = false) : m_metadata(metadata),
           m_fs(fs), m_interval(interval), m_dryrun(dryrun) {}

  TablePtr      m_metadata;
  Filesystem   *m_fs;
  int           m_interval;
  bool          m_dryrun;

  void
  scan_metadata() {
    TableMutatorPtr mutator_ptr;
    KeySpec key;
    TableScannerPtr scanner_ptr;
    ScanSpecificationT scan_spec;
    CellT cell;
    string location_str;
    CountMap files_map;

    scan_spec.rowLimit = 0;
    scan_spec.max_versions = 0;
    scan_spec.columns.clear();
    scan_spec.columns.push_back("Files");
    scan_spec.startRow = "";
    scan_spec.startRowInclusive = false;
    scan_spec.endRow = Key::END_ROW_MARKER;;
    scan_spec.endRowInclusive = false;
    scan_spec.interval.first = 0;
    scan_spec.interval.second = 0;

    int ret = m_metadata->create_scanner(scan_spec, scanner_ptr);

    if (ret != Error::OK) {
      HT_ERRORF("Error creating scanner on METADATA table: %s",
                Error::get_text(ret));
      return;
    }
    ret = m_metadata->create_mutator(mutator_ptr);

    if (ret != Error::OK) {
      HT_ERRORF("Error creating mutator on METADATA table: %s",
                Error::get_text(ret));
      return;
    }
    memset(&key, 0, sizeof(key));
    string last_row;
    string last_cq;
    uint64_t last_time = 0;
    bool found_valid_files = true;

    HT_DEBUG("TableFileGc: scanning metadata...");

    while (scanner_ptr->next(cell)) {
      if (last_row != cell.row_key) {
        // new row
        if (!found_valid_files && !last_row.empty()) {
          key.row = last_row.c_str();
          key.row_len = last_row.length();

          HT_DEBUGF("TableFileGc: Deleting row %s", (char *)key.row);

          if (!m_dryrun)
            mutator_ptr->set_delete(0, key);
        }
        last_row = cell.row_key;
        found_valid_files = false;
      }
      if (strcmp("Files", cell.column_family)) {
        HT_ERRORF("Unexpected column family '%s', while scanning METADATA", 
                  cell.column_family);
        continue;
      }
      if (last_cq != cell.column_qualifier) {
        // new access group
        last_cq = cell.column_qualifier;
        last_time = cell.timestamp;
        found_valid_files = *cell.value != '!';

        if (found_valid_files)
          insert_files(files_map, (char *)cell.value, cell.value_len, 1);
      }
      else {
        if (cell.timestamp > last_time) {
          HT_ERROR("Unexpected timestamp order while scanning METADATA");
          continue;
        }
        if (*cell.value != '!')
          insert_files(files_map, (char *)cell.value, cell.value_len);
      }
    }
    mutator_ptr->flush();
    reap(files_map);
  }

  void
  insert_files(CountMap &map, const char *buf, size_t len, int c = 0) {
    const char *p = buf, *p0 = p, *endp = p + len - 1;

    while (p < endp) {
      while (*p != ';' ||  p[1] != '\n' && p < endp)
        ++p;

      if (p == endp)
        break;

      string name(p0, p - p0);
      p += 2;
      CountMap::InsRet ret = map.insert(name.c_str(), c);

      if (!ret.second)
        (*ret.first).second += c;

      p0 = p;
    }
  }

  void
  reap(CountMap &map) {
    size_t n = 0;

    foreach (const CountMap::value_type &v, map) {
      if (!v.second) {
        // TODO: fs interface needs to be fixed to 
        // accept const char * and const string &
        string name(v.first);

        HT_DEBUGF("TableFileGc: removing file %s", v.first);

        if (!m_dryrun) {
          int ret = m_fs->remove(name);

          if (ret != Error::OK)
            HT_ERRORF("Error removing '%s'", v.first);
          else
            ++n;
        }
      }
    }
    HT_DEBUGF("TableFileGc: removed %lu files", n);
  }

  void
  operator()() {
    do {
      int remain = sleep(m_interval);

      if (remain)
        break; // interrupted       

      if (m_metadata) scan_metadata();

    } while (true);
  }
};

} // local namespace

namespace Hypertable {

void
start_table_file_gc(PropertiesPtr props, ThreadGroup &threads,
                    TablePtr metadata, Filesystem *fs) {
  int interval = props->get_int("Hypertable.TableFileGc.Interval", 300);

  threads.create_thread(GcWorker(metadata, fs, interval));
}

void
test_table_file_gc(TablePtr metadata, Filesystem *fs,
                   bool dryrun) {
  GcWorker gc(metadata, fs, 0, dryrun);
  gc.scan_metadata();
}

} // namespace Hypertable
