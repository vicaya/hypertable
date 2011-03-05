/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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
#include <cstdlib>
#include <iostream>

#include "Common/md5.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Future.h"

using namespace std;
using namespace Hypertable;

namespace {

  const char *schema =
  "<Schema>"
  "  <AccessGroup name=\"default\">"
  "    <ColumnFamily>"
  "      <Name>data</Name>"
  "    </ColumnFamily>"
  "  </AccessGroup>"
  "</Schema>";

  const char *usage[] = {
    "usage: future_test",
    "",
    "Validates asynchronous (Future) API and cancellation.",
    0
  };

  void load_buffer_with_random(uint8_t *buf, size_t size) {
    uint8_t *ptr = buf;
    uint32_t uival;
    size_t n = size / 4;
    if (size % 4)
      n++;
    for (size_t i=0; i<n; i++) {
      uival = (uint32_t)random();
      memcpy(ptr, &uival, 4);
      ptr += 4;
    }
  }

}


int main(int argc, char **argv) {
  unsigned long seed = 1234;
  uint8_t *buf = new uint8_t [1048576];
  char keybuf[32];
  uint8_t sent_digest[16];
  uint8_t received_digest[16];

  if (argc > 2 ||
      (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))))
    Usage::dump_and_exit(usage);

  if (argc == 2)
    seed = atoi(argv[1]);

  cout << "SEED: " << seed << endl;

  srandom(seed);

  try {
    Client *hypertable = new Client(argv[0], "./future_cancel_test.cfg");
    NamespacePtr ns = hypertable->open_namespace("/");

    TablePtr table_ptr;
    TableMutatorPtr mutator_ptr;
    KeySpec key;
    ScanSpecBuilder ssbuilder;
    ScanSpec scan_spec;
    Cell cell;
    md5_context md5_ctx;
    ResultPtr result;
    Cells cells;

    /**
     * Load data
     */
    ns->drop_table("FutureTest", true);
    ns->create_table("FutureTest", schema);

    table_ptr = ns->open_table("FutureTest");

    mutator_ptr = table_ptr->create_mutator();

    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    memset(&md5_ctx, 0, sizeof(md5_ctx));
    md5_starts(&md5_ctx);

    for (size_t i=0; i<100; i++) {
      load_buffer_with_random(buf, 1000);
      sprintf(keybuf, "%05u", (unsigned)i);
      key.row = keybuf;
      key.row_len = strlen(keybuf);
      mutator_ptr->set(key, buf, 1000);
      if (i<50)
        md5_update(&md5_ctx, buf, 1000);
    }
    md5_finish(&md5_ctx, sent_digest);

    mutator_ptr->flush();

    mutator_ptr = 0;

    {
      // Do asynchronous scan
      Future ff(5);
      TableScannerAsyncPtr scanner_ptr;

      ssbuilder.set_row_limit(60);
      ssbuilder.add_row_interval("00000",true, "00048", false);
      ssbuilder.add_row_interval("00048",true, "00049", false);
      ssbuilder.add_row_interval("00049",true, "00050", false);
      ssbuilder.add_row_interval("00050",true, "01000", false);
      scan_spec = ssbuilder.get();

      scanner_ptr = table_ptr->create_scanner_async(&ff, scan_spec);

      memset(&md5_ctx, 0, sizeof(md5_ctx));
      md5_starts(&md5_ctx);
      int num_cells=0;
      bool finished = false;
      while (!finished) {
        ff.get(result);
        if (result->is_error()) {
          int error;
          String error_msg;
          result->get_error(error, error_msg);
          Exception e(error, error_msg);
          HT_ERROR_OUT << "Encountered scan error " << e << HT_END;
          _exit(1);
        }

        result->get_cells(cells);
        for (size_t ii=0; ii< cells.size(); ++ii) {
          md5_update(&md5_ctx, (unsigned char *)cells[ii].value, cells[ii].value_len);
          num_cells++;
          if (num_cells>=50) {
            ff.cancel();
            finished=true;
            break;
          }
        }
      }
    }

    md5_finish(&md5_ctx, received_digest);

    if (memcmp(sent_digest, received_digest, 16)) {
      HT_ERROR("MD5 digest mismatch between sent and received");
      _exit(1);
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  _exit(0);
}
