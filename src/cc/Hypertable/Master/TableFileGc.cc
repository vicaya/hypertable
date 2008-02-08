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
#include <cstring>
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
  GcWorker(TablePtr &metadata, Filesystem *fs, int interval,
           bool dryrun = false) : m_metadata(metadata),
           m_fs(fs), m_interval(interval), m_dryrun(dryrun) {}

  TablePtr     &m_metadata;
  Filesystem   *m_fs;
  int           m_interval;
  bool          m_dryrun;

  void
  scan_metadata() {
    TableScannerPtr scanner;
    ScanSpecificationT scan_spec;

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

    int ret = m_metadata->create_scanner(scan_spec, scanner);

    if (ret != Error::OK) {
      HT_ERRORF("Error creating scanner on METADATA table: %s",
                Error::get_text(ret));
      return;
    }
    TableMutatorPtr mutator;
    ret = m_metadata->create_mutator(mutator);

    if (ret != Error::OK) {
      HT_ERRORF("Error creating mutator on METADATA table: %s",
                Error::get_text(ret));
      return;
    }
    CellT cell;
    string last_row;
    string last_cq;
    CountMap files_map;
    uint64_t last_time = 0;
    bool found_valid_files = true;
    KeySpec key;

    memset(&key, 0, sizeof(key));
    HT_DEBUG("TableFileGc: scanning metadata...");

    while (scanner->next(cell)) {
      if (strcmp("Files", cell.column_family)) {
        HT_ERRORF("Unexpected column family '%s', while scanning METADATA", 
                  cell.column_family);
        continue;
      }
      if (last_row != cell.row_key) {
        // new row
        if (!found_valid_files)
          delete_row(key, last_row, mutator);

        found_valid_files = *cell.value != '!';
        last_row = cell.row_key;
      }
      if (last_cq != cell.column_qualifier) {
        // new access group
        last_cq = cell.column_qualifier;
        last_time = cell.timestamp;
        int is_valid_files = *cell.value != '!';
        found_valid_files |= is_valid_files;

        if (is_valid_files)
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
    // for last table
    if (!found_valid_files)
      delete_row(key, last_row, mutator);

    mutator->flush();
    reap(files_map);
  }

  void
  delete_row(KeySpec &key, const string &row, TableMutatorPtr &mutator) {
    if (row.empty())
      return;

    key.row = row.c_str();
    key.row_len = row.length();

    HT_DEBUGF("TableFileGc: Deleting row %s", (char *)key.row);

    if (!m_dryrun)
      mutator->set_delete(0, key);
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
      insert_file(map, name.c_str(), c);
      p0 = p;
    }
  }

  void
  insert_file(CountMap &map, const char *fname, int c) {
    CountMap::InsRet ret = map.insert(fname, c);

    if (!ret.second)
      (*ret.first).second += c;
  }

  /**
   * Currently only stale cs files and range directories are reaped
   * Table directories probably should be obtained when removing
   * rows in METADATA
   */
  void
  reap(CountMap &files_map) {
    size_t nf = 0, nd = 0;
    CountMap dirs_map; // reap empty range directories as well
    int ret;

    foreach (const CountMap::value_type &v, files_map) {
      if (!v.second) {
        // TODO: fs interface needs to be fixed to 
        // accept const char * and const string &
        string name(v.first);

        HT_DEBUGF("TableFileGc: removing file %s", v.first);

        if (!m_dryrun) {
          if ((ret = m_fs->remove(name)) != Error::OK)
            HT_ERRORF("Error removing file '%s'", v.first);
          else
            ++nf;
        }
      }
      char *p = strrchr(v.first, '/');

      if (p) {
        string dir_name(v.first, p - v.first);
        insert_file(dirs_map, dir_name.c_str(), v.second);
      }
    }
    foreach (const CountMap::value_type &v, dirs_map) {
      if (!v.second) {
        // TODO: fs interface...
        string name(v.first);

        HT_DEBUGF("TableFileGc: removing directory %s", v.first);

        if (!m_dryrun) {
          if ((ret = m_fs->rmdir(name)) != Error::OK)
            HT_ERRORF("Error removing directory '%s'", v.first);
          else
            ++nd;
        }
      }
    }

    HT_DEBUGF("TableFileGc: removed %lu files; %lu directories", nf, nd);
  }

  void
  operator()() {
    do {
      int remain = sleep(m_interval);

      if (remain)
        break; // interrupted       

      if (m_metadata) scan_metadata();
      else HT_INFOF("TableFileGc: METADATA not ready, will try again in "
                    "%d seconds", m_interval);

    } while (true);
  }
};

} // local namespace

namespace Hypertable {

void
start_table_file_gc(PropertiesPtr props, ThreadGroup &threads,
                    TablePtr &metadata, Filesystem *fs) {
  int interval = props->get_int("Hypertable.TableFileGc.Interval", 300);

  threads.create_thread(GcWorker(metadata, fs, interval));

  HT_INFOF("Started table file garbage collection thread with interval: "
           "%d seconds", interval);
}

void
test_table_file_gc(TablePtr &metadata, Filesystem *fs,
                   bool dryrun) {
  GcWorker gc(metadata, fs, 0, dryrun);
  gc.scan_metadata();
}

} // namespace Hypertable
