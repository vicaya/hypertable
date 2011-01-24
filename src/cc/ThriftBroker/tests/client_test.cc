/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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
#include "Common/System.h"

#include <iostream>
#include <fstream>
#include "ThriftBroker/Client.h"
#include "ThriftBroker/gen-cpp/HqlService.h"
#include "ThriftBroker/ThriftHelper.h"
#include "ThriftBroker/SerializedCellsReader.h"

namespace Hypertable { namespace ThriftGen {

// Implement the If, so we don't miss any interface changes
struct BasicTest : HqlServiceIf {
  boost::shared_ptr<Thrift::Client> client;

  BasicTest() : client(new Thrift::Client("localhost", 38080)) { }

  void create_namespace(const std::string& ns) {
    client->create_namespace(ns);
  }

  void create_table(const Namespace ns, const std::string& table, const std::string& schema) {
    client->create_table(ns, table, schema);
  }

  Future open_future(int queue_size = 0) {
    return client->open_future(queue_size);
  }

  void get_future_result(Result& _result, const Future ff) {
    return client->get_future_result(_result, ff);
  }

  void get_future_result_as_arrays(ResultAsArrays& _result, const Future ff) {
    return client->get_future_result_as_arrays(_result, ff);
  }

  void get_future_result_serialized(ResultSerialized & _result, const Future ff) {
    return client->get_future_result_serialized(_result, ff);
  }

  void close_future(const Future ff) {
    client->close_future(ff);
  }

  Scanner open_scanner(const Namespace ns, const std::string& table,
                       const ScanSpec& scan_spec, bool retry_table_not_found = true) {
    return client->open_scanner(ns, table, scan_spec, retry_table_not_found);
  }

  ScannerAsync open_scanner_async(const Namespace ns, const std::string& table,
                                  const Future ff, const ScanSpec& scan_spec,
                                  bool retry_table_not_found = true) {
    return client->open_scanner_async(ns, table, ff, scan_spec, retry_table_not_found);
  }


  void close_scanner(const Scanner scanner) {
    client->close_scanner(scanner);
  }

  void close_scanner_async(const ScannerAsync scanner_async) {
    client->close_scanner_async(scanner_async);
  }

  void next_cells(std::vector<Cell> & _return, const Scanner scanner) {
    client->next_cells(_return, scanner);
  }

  void
  next_cells_as_arrays(std::vector<CellAsArray> & _return,
                       const Scanner scanner) {
    client->next_cells_as_arrays(_return, scanner);
  }

  void next_cells_serialized(CellsSerialized& _return,
                             const Scanner scanner) {
    client->next_cells_serialized(_return, scanner);
  }


  void next_row(std::vector<Cell> & _return, const Scanner scanner) {
    client->next_row(_return, scanner);
  }

  void
  next_row_as_arrays(std::vector<CellAsArray> & _return,
                     const Scanner scanner) {
    client->next_row_as_arrays(_return, scanner);
  }

  void
  get_row(std::vector<Cell> & _return, const Namespace ns, const std::string& table,
          const std::string& row) {
    client->get_row(_return, ns, table, row);
  }

  void next_row_serialized(CellsSerialized& _return,
                           const Scanner scanner) {
    client->next_row_serialized(_return, scanner);
  }

  void
  get_row_as_arrays(std::vector<CellAsArray> & _return, const Namespace ns,
      const std::string& table, const std::string& row) {
    client->get_row_as_arrays(_return, ns, table, row);
  }

  void get_row_serialized(CellsSerialized& _return, const Namespace ns,
                          const std::string& table, const std::string& row) {
    client->get_row_serialized(_return, ns, table, row);
  }

  void refresh_shared_mutator(const Namespace ns, const std::string &table,
                              const MutateSpec &mutate_spec) {
    client->refresh_shared_mutator(ns, table, mutate_spec);
  }

  void
  get_cell(Value& _return, const Namespace ns, const std::string& table,
           const std::string& row, const std::string& column) {
    client->get_cell(_return, ns, table, row, column);
  }

  void
  get_cells(std::vector<Cell> & _return, const Namespace ns, const std::string& table,
            const ScanSpec& scan_spec) {
    client->get_cells(_return, ns, table, scan_spec);
  }

  void
  get_cells_as_arrays(std::vector<CellAsArray> & _return, const Namespace ns,
      const std::string& table, const ScanSpec& scan_spec) {
    client->get_cells_as_arrays(_return, ns, table, scan_spec);
  }

  void
  get_cells_serialized(CellsSerialized& _return, const Namespace ns,
                       const std::string& table, const ScanSpec& scan_spec) {
    client->get_cells_serialized(_return, ns, table, scan_spec);
  }

  void offer_cell(const Namespace ns, const std::string &table, const MutateSpec &mutate_spec,
                const Cell &cell) {
    client->offer_cell(ns, table, mutate_spec, cell);
  }

  void offer_cells(const Namespace ns, const std::string &table, const MutateSpec &mutate_spec,
                 const std::vector<Cell> & cells) {
    client->offer_cells(ns, table, mutate_spec, cells);
  }

  void offer_cell_as_array(const Namespace ns, const std::string &table,
                         const MutateSpec &mutate_spec, const CellAsArray &cell) {
    client->offer_cell_as_array(ns, table, mutate_spec, cell);
  }

  void offer_cells_as_arrays(const Namespace ns, const std::string &table,
                           const MutateSpec &mutate_spec,const std::vector<CellAsArray> & cells) {
    client->offer_cells_as_arrays(ns, table, mutate_spec, cells);
  }

  Namespace open_namespace(const String &ns) {
    return client->open_namespace(ns);
  }

  void close_namespace(const Namespace ns) {
    client->close_namespace(ns);
  }

  Mutator open_mutator(const Namespace ns, const std::string& table, int32_t flags,
                       int32_t flush_interval = 0) {
    return client->open_mutator(ns, table, flags, flush_interval);
  }

  void close_mutator(const Mutator mutator, const bool flush) {
    client->close_mutator(mutator, flush);
  }

  void flush_mutator(const Mutator mutator) {
    client->flush_mutator(mutator);
  }

  void set_cell(const Mutator mutator, const Cell& cell) {
    client->set_cell(mutator, cell);
  }

  void set_cells(const Mutator mutator, const std::vector<Cell> & cells) {
    client->set_cells(mutator, cells);
  }

  void set_cell_as_array(const Mutator mutator, const CellAsArray& cell) {
    client->set_cell_as_array(mutator, cell);
  }

  void
  set_cells_as_arrays(const Mutator mutator,
                      const std::vector<CellAsArray> & cells) {
    client->set_cells_as_arrays(mutator, cells);
  }

  void
  set_cells_serialized(const Mutator mutator,
                       const CellsSerialized &cells,
                       bool flush) {
    client->set_cells_serialized(mutator, cells, flush);
  }

  bool exists_namespace(const std::string& ns) {
    return client->exists_namespace(ns);
  }

  bool exists_table(const Namespace ns, const std::string& table) {
    return client->exists_table(ns, table);
  }

  void get_table_id(std::string& _return, const Namespace ns, const std::string& table) {
    client->get_table_id(_return, ns, table);
  }

  void get_schema_str(std::string& _return, const Namespace ns, const std::string& table) {
    client->get_schema_str(_return, ns, table);
  }

  void get_schema(Schema& _return, const Namespace ns, const std::string& table) {
    client->get_schema(_return, ns, table);
  }

  void get_tables(std::vector<std::string> & _return, const Namespace ns) {
    client->get_tables(_return, ns);
  }

  void get_listing(std::vector<NamespaceListing> & _return, const Namespace ns) {
    client->get_listing(_return, ns);
  }

  void get_table_splits(std::vector<TableSplit> & _return, const Namespace ns,
                        const std::string& table) {
    client->get_table_splits(_return, ns, table);
  }

  void drop_namespace(const std::string& ns, const bool if_exists) {
    client->drop_namespace(ns, if_exists);
  }

  void rename_table(const Namespace ns, const std::string& table, const std::string& new_name) {
    client->rename_table(ns, table, new_name);
  }

  void drop_table(const Namespace ns, const std::string& table, const bool if_exists) {
    client->drop_table(ns, table, if_exists);
  }

  void
  hql_exec(HqlResult& _return, const Namespace ns, const std::string &command,
           const bool noflush, const bool unbuffered) {
    client->hql_exec(_return, ns, command, noflush, unbuffered);
  }

  void hql_query(HqlResult &ret, const Namespace ns, const std::string &command) {
    client->hql_query(ret, ns, command);
  }

  void
  hql_exec2(HqlResult2& _return, const Namespace ns, const std::string &command,
            const bool noflush, const bool unbuffered) {
    client->hql_exec2(_return, ns, command, noflush, unbuffered);
  }

  void hql_query2(HqlResult2 &ret, const Namespace ns, const std::string &command) {
    client->hql_query2(ret, ns, command);
  }

  void run() {
    try {
      std::ostream &out = std::cout;
      test_hql(out);
      test_schema(out);
      test_scan(out);
      test_set();
      test_put();
      test_scan(out);
      test_scan_async(out);
      test_rename();
    }
    catch (ClientException &e) {
      std::cout << e << std::endl;
      exit(1);
    }
  }

  void test_rename() {
    Namespace ns = open_namespace("test");
    HqlResult result;
    hql_query(result, ns, "create table foo('bar')");
    rename_table(ns, "foo", "foo_renamed");
    drop_table(ns, "foo_renamed", false);
    close_namespace(ns);
  }

  void test_hql(std::ostream &out) {
    HqlResult result;
    if (!exists_namespace("test"))
      create_namespace("test");
    Namespace ns = open_namespace("test");
    hql_query(result, ns, "drop table if exists thrift_test");
    hql_query(result, ns, "create table thrift_test ( col )");
    hql_query(result, ns, "insert into thrift_test values"
                      "('2008-11-11 11:11:11', 'k1', 'col', 'v1'), "
                      "('2008-11-11 11:11:11', 'k2', 'col', 'v2'), "
                      "('2008-11-11 11:11:11', 'k3', 'col', 'v3')");
    hql_query(result, ns, "select * from thrift_test");
    out << result << std::endl;

    HqlResult2 result2;
    hql_query2(result2, ns, "select * from thrift_test");
    out << result2 << std::endl;
    close_namespace(ns);
  }

  void test_scan(std::ostream &out) {
    ScanSpec ss;
    Namespace ns = open_namespace("test");

    Scanner s = open_scanner(ns, "thrift_test", ss);
    std::vector<Cell> cells;

    do {
      next_cells(cells, s);
      foreach(const Cell &cell, cells)
        out << cell << std::endl;
    } while (cells.size());

    close_scanner(s);

    ss.cell_limit=1;
    ss.__isset.cell_limit = true;
    ss.row_regexp = "k";
    ss.__isset.row_regexp = true;
    ss.value_regexp = "^v[24].*";
    ss.__isset.value_regexp = true;
    ss.columns.push_back("col");
    ss.__isset.columns = true;

    s = open_scanner(ns, "thrift_test", ss);
    do {
      next_cells(cells, s);
      foreach(const Cell &cell, cells)
        out << cell << std::endl;
    } while (cells.size());
    close_namespace(ns);
  }

  void test_set() {
    std::vector<Cell> cells;
    Namespace ns = open_namespace("test");

    Mutator m = open_mutator(ns, "thrift_test", 0);
    cells.push_back(make_cell("k4", "col", "cell_limit", "v-ignore-this-when-cell_limit=1",
                              "2008-11-11 22:22:22"));
    cells.push_back(make_cell("k4", "col", 0, "v4", "2008-11-11 22:22:23"));
    cells.push_back(make_cell("k5", "col", 0, "v5", "2008-11-11 22:22:22"));
    cells.push_back(make_cell("k2", "col", 0, "v2a", "2008-11-11 22:22:22"));
    cells.push_back(make_cell("k3", "col", 0, "", "2008-11-11 22:22:22", 0,
                              ThriftGen::KeyFlag::DELETE_ROW));
    set_cells(m, cells);
    close_mutator(m, true);
    close_namespace(ns);
  }

  void test_schema(std::ostream &out) {
    Schema schema;
    Namespace ns = open_namespace("test");
    get_schema(schema, ns, "thrift_test");
    close_namespace(ns);

    std::map<std::string, AccessGroup>::iterator ag_it = schema.access_groups.begin();
    out << "thrift test access groups:" << std::endl;
    while (ag_it != schema.access_groups.end()) {
      out << "\t" << ag_it->first << std::endl;
      ++ag_it;
    }
    std::map<std::string, ColumnFamily>::iterator cf_it = schema.column_families.begin();
    out << "thrift test column families:" << std::endl;
    while (cf_it != schema.column_families.end()) {
      out << "\t" << cf_it->first << std::endl;
      ++cf_it;
    }
  }

  void test_put() {
    std::vector<Cell> cells;
    MutateSpec mutate_spec;
    mutate_spec.appname = "test-cpp";
    mutate_spec.flush_interval = 1000;
    Namespace ns = open_namespace("test");

    cells.push_back(make_cell("put1", "col", 0, "v1", "2008-11-11 22:22:22"));
    cells.push_back(make_cell("put2", "col", 0, "this_will_be_deleted", "2008-11-11 22:22:22"));
    offer_cells(ns, "thrift_test", mutate_spec, cells);
    cells.clear();
    cells.push_back(make_cell("put1", "no_such_col", 0, "v1", "2008-11-11 22:22:22"));
    cells.push_back(make_cell("put2", "col", 0, "", "2008-11-11 22:22:23", 0,
                              ThriftGen::KeyFlag::DELETE_ROW));
    refresh_shared_mutator(ns, "thrift_test", mutate_spec);
    offer_cells(ns, "thrift_test", mutate_spec, cells);
    close_namespace(ns);
    sleep(2);
  }

  void test_scan_async(std::ostream &out) {
    Namespace ns = open_namespace("test");
    String insert;
    int num_expected_results = 6;
    int num_results = 0;
    HqlResult hql_result;
    hql_query(hql_result, ns, "create table FruitColor(data)");
    hql_query(hql_result, ns, "create table FruitLocation(data)");
    hql_query(hql_result, ns, "create table FruitEnergy(data)");
    insert = (String) "insert into FruitColor values ('apple', 'data', 'red'), " +
             "('kiwi', 'data', 'brown')";
    hql_query(hql_result, ns, insert.c_str());
    insert = (String) "insert into FruitLocation values ('apple', 'data', 'Western Asia'), " +
             "('kiwi','data', 'Southern China')";
    hql_query(hql_result, ns, insert.c_str());
    insert = (String) "insert into FruitEnergy values ('apple', 'data', '2.18kJ/g'), " +
             "('kiwi', 'data', '0.61Cal/g')";
    hql_query(hql_result, ns, insert.c_str());

    RowInterval ri_apple;
    ri_apple.start_row = "apple";
    ri_apple.__isset.start_row = true;
    ri_apple.end_row= "apple";
    ri_apple.__isset.end_row = true;

    RowInterval ri_kiwi;
    ri_kiwi.start_row = "kiwi";
    ri_kiwi.__isset.start_row = true;
    ri_kiwi.end_row = "kiwi";
    ri_kiwi.__isset.end_row = true;

    ScanSpec ss;
    ss.row_intervals.push_back(ri_apple);
    ss.row_intervals.push_back(ri_kiwi);
    ss.__isset.row_intervals = true;

    Future ff = open_future();
    ScannerAsync color_scanner     = open_scanner_async(ns, "FruitColor", ff, ss);
    ScannerAsync location_scanner  = open_scanner_async(ns, "FruitLocation", ff, ss);
    ScannerAsync energy_scanner    = open_scanner_async(ns, "FruitEnergy", ff, ss);

    Result result;
    ResultSerialized result_serialized;
    ResultAsArrays result_as_arrays;

    while (true) {
      if (num_results<2) {
        get_future_result(result, ff);
        if (result.is_empty)
          break;
        if (result.is_scan == false) {
          out << "All results are expected to be from scans" << std::endl;
          _exit(1);
        }
        if (result.is_error == true) {
          out << "Got unexpected error from async scan " << result.error_msg << std::endl;
          _exit(1);
        }

        if (result.id == color_scanner)
          out << "Got result from FruitColor: ";
        else if (result.id == location_scanner)
          out << "Got result from FruitLocation: ";
        else if (result.id == energy_scanner)
          out << "Got result from FruitEnergy: ;";
        else {
          out << "Got result from unknown scanner id " << result.id
            << " expecting one of " << color_scanner << ", " << location_scanner << ", "
            << energy_scanner << std::endl;
          _exit(1);
        }
        for (size_t ii=0; ii< result.cells.size(); ++ii) {
          out << result.cells[ii] << std::endl;
          num_results++;
        }
      }
      else if (num_results < 4) {
        get_future_result_as_arrays(result_as_arrays, ff);
        if (result_as_arrays.is_empty)
          break;
        if (result_as_arrays.is_scan == false) {
          out << "All results are expected to be from scans" << std::endl;
          _exit(1);
        }
        if (result_as_arrays.is_error == true) {
          out << "Got unexpected error from async scan " << result_as_arrays.error_msg
              << std::endl;
          _exit(1);
        }

        if (result_as_arrays.id == color_scanner)
          out << "Got result_as_arrays from FruitColor: ";
        else if (result_as_arrays.id == location_scanner)
          out << "Got result_as_arrays from FruitLocation: ";
        else if (result_as_arrays.id == energy_scanner)
          out << "Got result_as_arrays from FruitEnergy: ;";
        else {
          out << "Got result_as_arrays from unknown scanner id " << result.id
            << " expecting one of " << color_scanner << ", " << location_scanner << ", "
            << energy_scanner << std::endl;
          _exit(1);
        }
        for (size_t ii=0; ii < result_as_arrays.cells.size(); ++ii) {
          out << "{" ;
          for (size_t jj=0; jj < result_as_arrays.cells[ii].size(); ++jj) {
            if (jj > 0)
              out << ", ";
            out << (result_as_arrays.cells[ii])[jj];
          }
          out << "}" << std::endl;
          num_results++;
        }
      }
      else {
        get_future_result_serialized(result_serialized, ff);
        if (result_serialized.is_empty)
          break;
        if (result_serialized.is_scan == false) {
          out << "All results are expected to be from scans" << std::endl;
          _exit(1);
        }
        if (result_serialized.is_error == true) {
          out << "Got unexpected error from async scan " << result_serialized.error_msg
              << std::endl;
          _exit(1);
        }

        if (result_serialized.id == color_scanner)
          out << "Got result_serialized from FruitColor: ";
        else if (result_serialized.id == location_scanner)
          out << "Got result_serialized from FruitLocation: ";
        else if (result_serialized.id == energy_scanner)
          out << "Got result_serialized from FruitEnergy: ;";
        else {
          out << "Got result_serialized from unknown scanner id " << result_serialized.id
            << " expecting one of " << color_scanner << ", " << location_scanner << ", "
            << energy_scanner << std::endl;
          _exit(1);
        }
        SerializedCellsReader reader((void *)result_serialized.cells.c_str(),
                                     (uint32_t)result_serialized.cells.length());
        Hypertable::Cell hcell;
        while(reader.next()) {
          reader.get(hcell);
          out << hcell << std::endl;
          num_results++;
        }
      }
    }

    out << "Asynchronous scans finished" << std::endl;

    close_scanner_async(color_scanner);
    close_scanner_async(location_scanner);
    close_scanner_async(energy_scanner);
    close_future(ff);
    close_namespace(ns);
    if (num_results != num_expected_results) {
      out << "Expected " << num_expected_results << " received " << num_results << std::endl;
      _exit(1);
    }

  }
};

}} // namespace Hypertable::Thrift

int main() {
  Hypertable::ThriftGen::BasicTest test;
  test.run();
}
