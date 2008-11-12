#!/usr/bin/env thrift --gen cpp --gen java --gen perl --php --gen py --gen rb -r
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

namespace cpp   Hypertable.ThriftGen
namespace java  org.hypertable.thriftgen
namespace perl  Hypertable.ThriftGen
namespace php   Hypertable.ThriftGen
# python doesn't like multiple top dirs with the same name (say hypertable)
namespace py    hyperthrift.gen
namespace rb    Hypertable.ThriftGen

typedef i64 Scanner
typedef i64 Mutator
typedef binary Value

struct RowInterval {
  1: optional string start_row
  2: optional bool start_inclusive = 1
  3: optional string end_row    # 'end' chokes ruby
  4: optional bool end_inclusive = 1
}

struct CellInterval {
  1: optional string start_row
  2: optional string start_column
  3: optional bool start_inclusive = 1
  4: optional string end_row
  5: optional string end_column
  6: optional bool end_inclusive = 1
}

struct ScanSpec {
  1: optional list<RowInterval> row_intervals
  2: optional list<CellInterval> cell_intervals
  3: optional bool return_deletes = 0
  4: optional i32 revs = 0      // limit number revisions to return
  5: optional i32 row_limit = 0
  6: optional i64 start_time
  7: optional i64 end_time
}

// must be sync'ed with FLAG_* constants in Hypertable/Lib/Key.h
enum CellFlag {
  DELETE_ROW = 0,
  DELETE_CF = 1,
  DELETE_CELL = 2,
  INSERT = 255
}

struct Cell {
  1: optional string row_key
  2: optional string column_family
  3: optional string column_qualifier
  4: optional Value value
  5: optional i64 timestamp
  6: optional i64 revision
  7: optional i16 flag = INSERT
}

// some languages (like php) don't have adequate namespace so Exception would
// conflict with language builtins
exception ClientException {
  1: i32 code
  2: string what
}

/**
 * The client service mimics the C++ client API, with table, scanner and
 * mutator interface flattened.
 */
service ClientService {

  /**
   * Create a table
   * @param name - table name
   * @param schema - schema of the table (in xml)
   */
  void create_table(1:string name, 2:string schema)
      throws (1:ClientException e),

  /**
   * Open a table scanner
   * @param name - table name
   * @param scan_spec - scan specification
   * @return scanner id
   */
  Scanner open_scanner(1:string name, 2:ScanSpec scan_spec)
      throws (1:ClientException e),

  /**
   * Close a table scanner
   * @param scanner - scanner id to close
   */
  void close_scanner(1:Scanner scanner) throws (1:ClientException e),

  /**
   * Iterate over cells of a scanner
   * @param scanner - scanner id
   * @return a list of cells
   */
  list<Cell> next_cells(1:Scanner scanner) throws (1:ClientException e),

  /**
   * Get a row (convenience method for random access a row)
   * @param name - table name
   * @param row - row key
   * @return a list of cells (with row_keys unset)
   */
  list<Cell> get_row(1:string name, 2:string row) throws (1:ClientException e),

  /**
   * Get a cell (convenience method for random access a cell)
   * @param name - table name
   * @param row - row key
   * @param column - column name
   * @return value (byte sequence)
   */
  Value get_cell(1:string name, 2:string row, 3:string column)
      throws (1:ClientException e),

  /**
   * Get cells (convenience method for access small amount of cells)
   * @param name - table name
   * @param scan_spec - scan specification
   * @return a list of cells
   */
  list<Cell> get_cells(1:string name, 2:ScanSpec scan_spec)
      throws (1:ClientException e),

  /**
   * Open a table mutator
   * @param name - table name
   * @return mutator id
   */
  Mutator open_mutator(1:string name) throws (1:ClientException e),

  /**
   * Close a table mutator
   * @param mutator - mutator id to close
   */
  void close_mutator(1:Mutator mutator, 2:bool flush = 1)
      throws (1:ClientException e),

  /**
   * Set a cell in the table
   * @param mutator - mutator id
   * @param cell - the cell to set
   */
  void set_cell(1:Mutator mutator, 2:Cell cell) throws (1:ClientException e),

  /**
   * Put a list of cells into a table
   * @param mutator - mutator id
   * @param cells - a list of cells
   */
  void set_cells(1:Mutator mutator, 2:list<Cell> cells)
      throws (1:ClientException e),

  /**
   * Flush mutator buffers
   */
  void flush_mutator(1:Mutator mutator) throws (1:ClientException e),

  /**
   * Get the id of a table
   * @param name - table name
   * @return table id
   */
  i32 get_table_id(1:string name) throws (1:ClientException e),

  /**
   * Get the schema of a table (that can be used with creat_table)
   * @param name - table name
   * @return schema string (in xml)
   */
  string get_schema(1:string name) throws (1:ClientException e),

  /**
   * Get a list of table names in the cluster
   * @return a list of table names
   */
  list<string> get_tables() throws (1:ClientException e),

  /**
   * Drop a table
   * @param name - table name
   * @param if_exists - don't barf if table doesn't exist
   */
  void drop_table(1:string name, 2:bool if_exists = 1)
      throws (1:ClientException e),
}
