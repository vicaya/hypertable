/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Hypertable/Lib/Defaults.h"

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
    "usage: large_insert_test [<seed>]",
    "",
    "Validates the insertion and retrieval of large items.",
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
  Client *hypertable;
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

  hypertable = new Client(argv[0], "./hypertable.cfg");

  try {
    TablePtr table_ptr;
    TableMutatorPtr mutator_ptr;
    TableScannerPtr scanner_ptr;
    KeySpec key;
    ScanSpec scan_spec;
    Cell cell;
    md5_context md5_ctx;

    /**
     * Validate large object returned by CREATE_SCANNER
     */
    hypertable->drop_table("BigTest", true);
    hypertable->create_table("BigTest", schema);

    table_ptr = hypertable->open_table("BigTest");

    mutator_ptr = table_ptr->create_mutator();

    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    memset(&md5_ctx, 0, sizeof(md5_ctx));
    md5_starts(&md5_ctx);

    /** Add a big one **/
    load_buffer_with_random(buf, 1000000);
    sprintf(keybuf, "%05u", (unsigned)1001);
    key.row = keybuf;
    key.row_len = strlen(keybuf);
    mutator_ptr->set(key, buf, 1000000);
    md5_update(&md5_ctx, buf, 1000000);

    md5_finish(&md5_ctx, sent_digest);

    mutator_ptr->flush();

    mutator_ptr = 0;

    scanner_ptr = table_ptr->create_scanner(scan_spec);

    memset(&md5_ctx, 0, sizeof(md5_ctx));
    md5_starts(&md5_ctx);

    while (scanner_ptr->next(cell))
      md5_update(&md5_ctx, (unsigned char *)cell.value, cell.value_len);

    md5_finish(&md5_ctx, received_digest);

    if (memcmp(sent_digest, received_digest, 16)) {
      HT_ERROR("MD5 digest mismatch between sent and received");
      return 1;
    }

    scanner_ptr = 0;
    table_ptr = 0;


    /**
     * Validate large object returned by FETCH_SCANBLOCK
     */

    hypertable->drop_table("BigTest", true);
    hypertable->create_table("BigTest", schema);

    table_ptr = hypertable->open_table("BigTest");

    mutator_ptr = table_ptr->create_mutator();

    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    memset(&md5_ctx, 0, sizeof(md5_ctx));
    md5_starts(&md5_ctx);

    for (size_t i=0; i<1000; i++) {
      load_buffer_with_random(buf, 1000);
      sprintf(keybuf, "%05u", (unsigned)i);
      key.row = keybuf;
      key.row_len = strlen(keybuf);
      mutator_ptr->set(key, buf, 1000);
      md5_update(&md5_ctx, buf, 1000);
    }

    /** Add a big one **/
    load_buffer_with_random(buf, 1000000);
    sprintf(keybuf, "%05u", (unsigned)1001);
    key.row = keybuf;
    key.row_len = strlen(keybuf);
    mutator_ptr->set(key, buf, 1000000);
    md5_update(&md5_ctx, buf, 1000000);

    md5_finish(&md5_ctx, sent_digest);

    mutator_ptr->flush();

    mutator_ptr = 0;

    scanner_ptr = table_ptr->create_scanner(scan_spec);

    memset(&md5_ctx, 0, sizeof(md5_ctx));
    md5_starts(&md5_ctx);

    while (scanner_ptr->next(cell))
      md5_update(&md5_ctx, (unsigned char *)cell.value, cell.value_len);

    md5_finish(&md5_ctx, received_digest);

    if (memcmp(sent_digest, received_digest, 16)) {
      HT_ERROR("MD5 digest mismatch between sent and received");
      return 1;
    }

    scanner_ptr = 0;
    table_ptr = 0;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }

  return 0;
}
