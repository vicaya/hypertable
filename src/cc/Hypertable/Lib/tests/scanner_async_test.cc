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
#include <fstream>
#include <vector>

#include "Common/StringExt.h"
#include "Common/Usage.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/Client.h"

using namespace std;
using namespace Hypertable;

namespace {

  ofstream outfile;

  const char *fruits[] = {
    "apple",
    "banana",
    "pomegranate",
    "kiwi",
    "guava",
    (const char*) 0
  };

  const char *locations[] = {
    "Western Asia",
    "Papua New Guinea",
    "Iranian plateau",
    "Southern China",
    (const char *) 0
  };

  const char *energy_densities[] = {
    "2.18kJ/g",
    "0.89Cal/g",
    "0.83Cal/g",
    "0.61Cal/g",
    (const char*) 0
  };

  const char *colors[] = {
    "red",
    "yellow",
    "red",
    "brown",
    (const char *) 0
  };

  const char *schema =
  "<Schema>"
  "  <AccessGroup name=\"default\">"
  "    <ColumnFamily>"
  "      <Name>data</Name>"
  "    </ColumnFamily>"
  "  </AccessGroup>"
  "</Schema>";

  const char *usage[] = {
    "usage: scanner_async_test",
    "",
    "Tests asynchronous scans across multiple tables. Generates output file "
    "'./scannerAsyncTest.output' and diffs it against './scannerAsyncTest.golden'.'",
    0
  };

  String output;
  class Fruit {
  public:
    Fruit() {}
    String &to_str() {
      str = (String)"{name:" + name + ", location:"+ location + ", color:" + color +
            ", energy_density:" + energy_density + "}";
      return str;
    }
    void clear() {
      str.clear();
      name.clear();
      location.clear();
      color.clear();
      energy_density.clear();
    }
    String str;
    String name;
    String location;
    String color;
    String energy_density;
  };

  class FruitCallback: public ResultCallback {
  public:
    FruitCallback():m_error(Error::OK) {}

    void clear() {
      m_fruit.clear();
    }

    void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells) {
      ScopedLock lock(mutex);
      Cells cc;
      cells->get(cc);
      String table_name = scanner->get_table_name();

      if (cc.size() == 1) {
        if (!table_name.compare("FruitColor")) {
          m_fruit.name = cc[0].row_key;
          m_fruit.color = String((const char *)cc[0].value, cc[0].value_len);
        }
        else if (!table_name.compare("FruitLocation")) {
          m_fruit.name = cc[0].row_key;
          m_fruit.location = String((const char *)cc[0].value, cc[0].value_len);
        }
        else if (!table_name.compare("FruitEnergy")) {
          m_fruit.name = cc[0].row_key;
          m_fruit.energy_density = String((const char *)cc[0].value, cc[0].value_len);
        }
        else {
          outfile << "Received unexpected cell " << cc[0] << endl;

        }
      }
      else {
        outfile << "Received "<< cc.size() << "cells for scan " << endl;
      }
    }

    void scan_error(TableScannerAsync *scanner, int error, const String &error_msg, bool eos) {
      ScopedLock lock(mutex);
      Exception e(error, error_msg);
      outfile << e << endl;
      _exit(1);
    }
    void update_ok(TableMutator *mutator, FailedMutations &failed_mutations) {
      outfile << "This should never happen" << endl;
      _exit(1);
    }
    void update_error(TableMutator *mutator, int error, const String &error_msg) {
      outfile << "This should never happen" << endl;
      _exit(1);
    }

    String &get_fruit() {
      return m_fruit.to_str();
    }

  private:
    Mutex mutex;
    Fruit m_fruit;
    int m_error;
    String m_error_msg;
  };

  void read(Client *client, NamespacePtr &ns) {
    TablePtr table_color;
    TablePtr table_location;
    TablePtr table_energy;

    FruitCallback cb;
    ScanSpecBuilder ss;
    TableScannerAsyncPtr scanner_color;
    TableScannerAsyncPtr scanner_location;
    TableScannerAsyncPtr scanner_energy;

    table_color       = ns->open_table("FruitColor");
    table_location    = ns->open_table("FruitLocation");
    table_energy      = ns->open_table("FruitEnergy");

    for(int ii=0; ii<5 ;++ii) {
      ss.clear();
      cb.clear();
      ss.add_row(fruits[ii]);
      outfile << "Issuing scans for '" << fruits[ii] << "'" << endl;
      scanner_color     = table_color->create_scanner_async(&cb, ss.get());
      scanner_location  = table_location->create_scanner_async(&cb, ss.get());
      scanner_energy    = table_energy->create_scanner_async(&cb, ss.get());
      cb.wait_for_completion();
      outfile << "Got result = " << cb.get_fruit() << endl;
    }

  }

  void write(Client *client, NamespacePtr &ns) {
    KeySpec key;
    TablePtr table_color;
    TablePtr table_location;
    TablePtr table_energy;

    TableMutatorPtr mutator_color;
    TableMutatorPtr mutator_location;
    TableMutatorPtr mutator_energy;

    ns->drop_table("FruitColor", true);
    ns->drop_table("FruitLocation", true);
    ns->drop_table("FruitEnergy", true);

    ns->create_table("FruitColor", schema);
    ns->create_table("FruitLocation", schema);
    ns->create_table("FruitEnergy", schema);

    table_color       = ns->open_table("FruitColor");
    table_location    = ns->open_table("FruitLocation");
    table_energy      = ns->open_table("FruitEnergy");

    mutator_color     = table_color->create_mutator();
    mutator_location  = table_location->create_mutator();
    mutator_energy    = table_energy->create_mutator();

    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    for(int ii=0; ii<4 ;++ii) {
      key.row = fruits[ii];
      key.row_len = strlen(fruits[ii]);

      mutator_color->set(key, (uint8_t *)colors[ii], strlen(colors[ii]));
      mutator_color->flush();
      mutator_location->set(key, (uint8_t *)locations[ii], strlen(locations[ii]));
      mutator_location->flush();
      mutator_energy->set(key, (uint8_t *)energy_densities[ii], strlen(energy_densities[ii]));
      mutator_energy->flush();
    }
  }

}


int main(int argc, char **argv) {

  if (argc > 1 ||
      (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))))
    Usage::dump_and_exit(usage);

  try {
    Client *hypertable = new Client(argv[0], "./hypertable.cfg");
    NamespacePtr ns = hypertable->open_namespace("/");

    outfile.open("scannerAsyncTest.output");
    write(hypertable, ns);
    read(hypertable, ns);
    outfile.close();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  if (system("diff ./scannerAsyncTest.output ./scannerAsyncTest.golden"))
    _exit(1);

  _exit(0);
}
