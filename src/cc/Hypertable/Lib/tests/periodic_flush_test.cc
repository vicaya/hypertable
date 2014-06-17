/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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

#include "Common/Compat.h"
#include "Common/Init.h"

#include <unistd.h>

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlInterpreter.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

void check_results(Table *table) {
  ScanSpec ss;
  TableScannerPtr scanner = table->create_scanner(ss);
  CellsBuilder cb;

  copy(scanner, cb);

  HT_ASSERT(cb.get().size() == 1);
  HT_ASSERT(cb.get().front().value_len == 5);
  HT_ASSERT(memcmp(cb.get().front().value, "value", 5) == 0);
}

void default_test(Table *table)  {
  TableMutatorPtr mutator = table->create_mutator(0, 0, 500);
  mutator->set(KeySpec("rowkey", "col", "cq"), "value");
  sleep(1);
  check_results(table);
}

void no_log_sync_test(Table *table) {
  TableMutatorPtr mutator = table->create_mutator(0, TableMutator::FLAG_NO_LOG_SYNC, 500);
  mutator->set_delete(KeySpec("rowkey", "col"));
  mutator->set(KeySpec("rowkey", "col", "cq"), "value");
  sleep(1);
  check_results(table);
}

} // local namesapce


int main(int argc, char *argv[]) {
  try {
    init_with_policy<DefaultClientPolicy>(argc, argv);

    ClientPtr client = new Hypertable::Client();
    NamespacePtr ns = client->open_namespace("/");
    HqlInterpreterPtr hql = client->create_hql_interpreter();

    hql->execute("use '/'");
    hql->execute("drop table if exists periodic_flush_test");
    hql->execute("create table periodic_flush_test(col)");

    TablePtr table = ns->open_table("periodic_flush_test");

    default_test(table.get());
    no_log_sync_test(table.get());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  _exit(0);
}
