/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_HQLPARSER_H
#define HYPERTABLE_HQLPARSER_H

//#define BOOST_SPIRIT_DEBUG  ///$$$ DEFINE THIS WHEN DEBUGGING $$$///

#ifdef BOOST_SPIRIT_DEBUG
#define BOOST_SPIRIT_DEBUG_OUT std::cerr
#define HQL_DEBUG(_expr_) std::cerr << __func__ <<": "<< _expr_ << std::endl
#define HQL_DEBUG_VAL(str, val) HQL_DEBUG(str <<" val="<< val)
#else
#define HQL_DEBUG(_expr_)
#define HQL_DEBUG_VAL(str, val)
#endif

#include <boost/algorithm/string.hpp>
#include <boost/spirit/core.hpp>
#include <boost/spirit/symbols/symbols.hpp>

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <vector>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"

#include "Key.h"
#include "Cells.h"
#include "Schema.h"
#include "ScanSpec.h"
#include "LoadDataSource.h"

namespace Hypertable {
  namespace Hql {
    using namespace boost;
    using namespace spirit;

    enum {
      COMMAND_HELP=1,
      COMMAND_CREATE_TABLE,
      COMMAND_DESCRIBE_TABLE,
      COMMAND_SHOW_CREATE_TABLE,
      COMMAND_SELECT,
      COMMAND_LOAD_DATA,
      COMMAND_INSERT,
      COMMAND_DELETE,
      COMMAND_SHOW_TABLES,
      COMMAND_DROP_TABLE,
      COMMAND_ALTER_TABLE,
      COMMAND_CREATE_SCANNER,
      COMMAND_DESTROY_SCANNER,
      COMMAND_FETCH_SCANBLOCK,
      COMMAND_LOAD_RANGE,
      COMMAND_SHUTDOWN,
      COMMAND_UPDATE,
      COMMAND_REPLAY_BEGIN,
      COMMAND_REPLAY_LOAD_RANGE,
      COMMAND_REPLAY_LOG,
      COMMAND_REPLAY_COMMIT,
      COMMAND_DROP_RANGE,
      COMMAND_DUMP,
      COMMAND_CLOSE,
      COMMAND_MAX
    };

   enum {
      RELOP_EQ=1,
      RELOP_LT,
      RELOP_LE,
      RELOP_GT,
      RELOP_GE,
      RELOP_SW
    };

    enum {
      ALTER_ADD=1,
      ALTER_DROP
    };

    class InsertRecord {
    public:
      InsertRecord() : timestamp(AUTO_ASSIGN) { }
      void clear() {
        timestamp = AUTO_ASSIGN;
        row_key.clear();
        column_key.clear();
        value.clear();
      }
      uint64_t timestamp;
      String row_key;
      String column_key;
      String value;
    };

    class RowInterval {
    public:
      RowInterval() : start_inclusive(true), start_set(false),
          end(Key::END_ROW_MARKER), end_inclusive(true), end_set(false) { }

      void clear() {
        start.clear();
        end = Key::END_ROW_MARKER;
        start_inclusive = end_inclusive = true;
        start_set = end_set = false;
      }
      bool empty() { return !(start_set || end_set); }

      void set_start(const String &s, bool inclusive) {
        HQL_DEBUG(s <<" inclusive="<< inclusive);
        start = s;
        start_inclusive = inclusive;
        start_set = true;
      }
      void set_end(const String &s, bool inclusive) {
        HQL_DEBUG(s <<" inclusive="<< inclusive);
        end = s;
        end_inclusive = inclusive;
        end_set = true;
      }
      String start;
      bool start_inclusive;
      bool start_set;
      String end;
      bool end_inclusive;
      bool end_set;
    };

    class CellInterval {
    public:
      CellInterval() : start_inclusive(true), start_set(false),
          end_inclusive(true), end_set(false) { }

      void clear() {
        start_row = start_column = "";
        end_row = end_column = "";
        start_inclusive = end_inclusive = true;
        start_set = end_set = false;
      }

      bool empty() { return !(start_set || end_set); }

      void set_start(const String &row, const String &column, bool inclusive) {
        HQL_DEBUG(row <<','<< column <<" inclusive="<< inclusive);
        start_row = row;
        start_column = column;
        start_inclusive = inclusive;
        start_set = true;
      }
      void set_end(const String &row, const String &column, bool inclusive) {
        HQL_DEBUG(row <<','<< column <<" inclusive="<< inclusive);
        end_row = row;
        end_column = column;
        end_inclusive = inclusive;
        end_set = true;
      }
      String start_row;
      String start_column;
      bool start_inclusive;
      bool start_set;
      String end_row;
      String end_column;
      bool end_inclusive;
      bool end_set;
    };

    class ScanState {
    public:
      ScanState() : display_timestamps(false), keys_only(false),
          current_rowkey_set(false), start_time_set(false),
          end_time_set(false), current_timestamp_set(false),
          current_relop(0) { }

      void set_time_interval(int64_t start, int64_t end) {
        HQL_DEBUG("("<< start <<", "<< end <<")");
        builder.set_time_interval(start, end);
        start_time_set = end_time_set = true;
      }
      void set_start_time(int64_t start) {
        HQL_DEBUG(start);
        builder.set_start_time(start);
        start_time_set = true;
      }
      void set_end_time(int64_t end) {
        HQL_DEBUG(end);
        builder.set_end_time(end);
        end_time_set = true;
      }
      int64_t start_time() { return builder.get().time_interval.first; }
      int64_t end_time() { return builder.get().time_interval.second; }

      ScanSpecBuilder builder;
      String outfile;
      bool display_timestamps;
      bool keys_only;
      String current_rowkey;
      bool current_rowkey_set;
      RowInterval current_ri;
      CellInterval current_ci;
      String current_cell_row;
      String current_cell_column;
      bool    start_time_set;
      bool    end_time_set;
      int64_t current_timestamp;
      bool    current_timestamp_set;
      int current_relop;
    };

    class ParserState {
    public:
      ParserState() : command(0), table_blocksize(0), table_in_memory(false),
                      max_versions(0), ttl(0), dupkeycols(false), cf(0), ag(0),
                      nanoseconds(0), delete_all_columns(false), delete_time(0),
                      if_exists(false), with_ids(false), replay(false),
                      scanner_id(-1), row_uniquify_chars(0), escape(true),
		      nokeys(false) {
        memset(&tmval, 0, sizeof(tmval));
      }
      int command;
      String table_name;
      String clone_table_name;
      String str;
      String output_file;
      String input_file;
      int input_file_src;
      String header_file;
      int header_file_src;
      String table_compressor;
      uint32_t table_blocksize;
      bool table_in_memory;
      uint32_t max_versions;
      time_t   ttl;
      std::vector<String> key_columns;
      String timestamp_column;
      bool dupkeycols;
      Schema::ColumnFamily *cf;
      Schema::AccessGroup *ag;
      Schema::ColumnFamilyMap cf_map;
      Schema::AccessGroupMap ag_map;
      Schema::ColumnFamilies cf_list;   // preserve order
      Schema::AccessGroups ag_list;     // ditto
      struct tm tmval;
      uint32_t nanoseconds;
      ScanState scan;
      InsertRecord current_insert_value;
      CellsBuilder inserts;
      std::vector<String> delete_columns;
      bool delete_all_columns;
      String delete_row;
      uint64_t delete_time;
      bool if_exists;
      bool with_ids;
      bool replay;
      String range_start_row;
      String range_end_row;
      int32_t scanner_id;
      int32_t row_uniquify_chars;
      bool escape;
      bool nokeys;
    };

    struct set_command {
      set_command(ParserState &state, int cmd) : state(state), command(cmd) { }
      void operator()(char const *, char const *) const {
        state.command = command;
      }
      ParserState &state;
      int command;
    };

    struct set_table_name {
      set_table_name(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.table_name = String(str, end-str);
        trim_if(state.table_name, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_clone_table_name {
      set_clone_table_name(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.clone_table_name = String(str, end-str);
        trim_if(state.clone_table_name, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_range_start_row {
      set_range_start_row(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.range_start_row = String(str, end-str);
        trim_if(state.range_start_row, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_range_end_row {
      set_range_end_row(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.range_end_row = String(str, end-str);
        trim_if(state.range_end_row, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_if_exists {
      set_if_exists(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.if_exists = true;
      }
      ParserState &state;
    };

    struct set_with_ids {
      set_with_ids(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.with_ids = true;
      }
      ParserState &state;
    };

    struct set_replay {
      set_replay(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.replay = true;
      }
      ParserState &state;
    };

    struct create_column_family {
      create_column_family(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.cf = new Schema::ColumnFamily();
        state.cf->name = String(str, end-str);
        state.cf->deleted = false;
        trim_if(state.cf->name, is_any_of("'\""));
        Schema::ColumnFamilyMap::const_iterator iter =
            state.cf_map.find(state.cf->name);
        if (iter != state.cf_map.end())
          HT_THROW(Error::HQL_PARSE_ERROR, String("Column family '") +
                   state.cf->name + " multiply defined.");
        state.cf_map[state.cf->name] = state.cf;
        state.cf_list.push_back(state.cf);
      }
      ParserState &state;
    };

    struct drop_column_family {
      drop_column_family(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.cf = new Schema::ColumnFamily();
        state.cf->name = String(str, end-str);
        state.cf->deleted = true;
        trim_if(state.cf->name, is_any_of("'\""));
        Schema::ColumnFamilyMap::const_iterator iter =
            state.cf_map.find(state.cf->name);
        if (iter != state.cf_map.end())
          HT_THROW(Error::HQL_PARSE_ERROR, String("Column family '") +
                   state.cf->name + " multiply dropped.");
        state.cf_map[state.cf->name] = state.cf;
        state.cf_list.push_back(state.cf);
      }
      ParserState &state;
    };

    struct set_max_versions {
      set_max_versions(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        if (state.cf == 0)
          state.max_versions = (uint32_t)strtol(str, 0, 10);
        else
          state.cf->max_versions = (uint32_t)strtol(str, 0, 10);
      }
      ParserState &state;
    };

    struct set_ttl {
      set_ttl(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        char *unit_ptr;
        double ttl = strtod(str, &unit_ptr);

        while(*unit_ptr == ' ' &&  unit_ptr < end )
          ++unit_ptr;

        String unit_str = String(unit_ptr, end-unit_ptr);
        to_lower(unit_str);

        if (unit_str.find("month") == 0)
          ttl *= 2592000.0;
        else if (unit_str.find("week") == 0)
          ttl *= 604800.0;
        else if (unit_str.find("day") == 0)
          ttl *= 86400.0;
        else if (unit_str.find("hour") == 0)
          ttl *= 3600.0;
        else if (unit_str.find("minute") == 0)
          ttl *= 60.0;

        if (state.cf == 0)
          state.ttl = (time_t)ttl;
        else
          state.cf->ttl = (time_t)ttl;
      }
      ParserState &state;
    };

    struct create_access_group {
      create_access_group(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String name(str, end-str);
        trim_if(name, is_any_of("'\""));
        Schema::AccessGroupMap::const_iterator iter = state.ag_map.find(name);

        if (iter != state.ag_map.end())
          state.ag = (*iter).second;
        else {
          state.ag = new Schema::AccessGroup();
          state.ag->name = name;
          state.ag_map[state.ag->name] = state.ag;
          state.ag_list.push_back(state.ag);
        }
      }
      ParserState &state;
    };

    struct set_access_group_in_memory {
      set_access_group_in_memory(ParserState &state) : state(state) { }
      void operator()(char const *, char const *) const {
        state.ag->in_memory=true;
      }
      ParserState &state;
    };

    struct set_access_group_compressor {
      set_access_group_compressor(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.ag->compressor = String(str, end-str);
        trim_if(state.ag->compressor, is_any_of("'\""));
        to_lower(state.ag->compressor);
      }
      ParserState &state;
    };

    struct set_access_group_blocksize {
      set_access_group_blocksize(ParserState &state) : state(state) { }
      void operator()(size_t blocksize) const {
        state.ag->blocksize = blocksize;
      }
      ParserState &state;
    };

    struct set_access_group_bloom_filter {
      set_access_group_bloom_filter(ParserState &state) : state(state) { }
      void operator()(char const * str, char const *end) const {
        state.ag->bloom_filter = String(str, end-str);
        trim_if(state.ag->bloom_filter, boost::is_any_of("'\""));
        to_lower(state.ag->bloom_filter);
      }
      ParserState &state;
    };

    struct add_column_family {
      add_column_family(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String name(str, end-str);
        trim_if(name, is_any_of("'\""));
        Schema::ColumnFamilyMap::const_iterator iter = state.cf_map.find(name);
        if (iter == state.cf_map.end())
          HT_THROW(Error::HQL_PARSE_ERROR, String("Access Group '")
                   + state.ag->name + "' includes unknown column family '"
                   + name + "'");
        if ((*iter).second->ag != "" && (*iter).second->ag != state.ag->name)
          HT_THROW(Error::HQL_PARSE_ERROR, String("Column family '") + name
                   + "' can belong to only one access group");
        (*iter).second->ag = state.ag->name;
      }
      ParserState &state;
    };

    struct set_table_compressor {
      set_table_compressor(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        if (state.table_compressor != "")
          HT_THROW(Error::HQL_PARSE_ERROR, "table compressor multiply defined");
        state.table_compressor = String(str, end-str);
        trim_if(state.table_compressor, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_table_in_memory {
      set_table_in_memory(ParserState &state) : state(state) { }
      void operator()(char const *, char const *) const {
        state.table_in_memory=true;
      }
      ParserState &state;
    };

    struct set_table_blocksize {
      set_table_blocksize(ParserState &state) : state(state) { }
      void operator()(size_t blocksize) const {
        state.table_blocksize = blocksize;
      }
      ParserState &state;
    };

    struct set_help {
      set_help(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.command = COMMAND_HELP;
        state.str = String(str, end-str);
        size_t offset = state.str.find_first_of(' ');
        if (offset != String::npos) {
          state.str = state.str.substr(offset+1);
          trim(state.str);
          to_lower(state.str);
        }
        else
          state.str = "";
      }
      ParserState &state;
    };

    struct set_str {
      set_str(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.str = String(str, end-str);
        trim_if(state.str, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_output_file {
      set_output_file(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.output_file = String(str, end-str);
        trim_if(state.output_file, is_any_of("'\""));
        FileUtils::expand_tilde(state.output_file);
      }
      ParserState &state;
    };

    struct set_input_file {
      set_input_file(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String file = String(str, end-str);
        trim_if(file, is_any_of("'\""));

        if (boost::algorithm::starts_with(file, "dfs://")) {
          state.input_file_src = DFS_FILE;
          state.input_file = file.substr(6);
        }
        else if (file.compare("-") == 0) {
          state.input_file_src = STDIN;
        }
        else {
          state.input_file_src = LOCAL_FILE;
          if (boost::algorithm::starts_with(file, "file://"))
            state.input_file = file.substr(7);
          else
            state.input_file = file;
          FileUtils::expand_tilde(state.input_file);
        }
      }

      ParserState &state;
    };

    struct set_header_file {
      set_header_file(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String file = String(str, end-str);
        trim_if(file, is_any_of("'\""));

        if (boost::algorithm::starts_with(file, "dfs://")) {
          state.header_file_src = DFS_FILE;
          state.header_file = file.substr(6);
        }
        else {
          state.header_file_src = LOCAL_FILE;
          if (boost::algorithm::starts_with(file, "file://"))
            state.header_file = file.substr(7);
          else
            state.header_file = file;
          FileUtils::expand_tilde(state.header_file);
        }
      }
      ParserState &state;
    };

    struct set_dup_key_cols {
      set_dup_key_cols(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.dupkeycols = *str != '0' && strncasecmp(str, "no", 2)
            && strncasecmp(str, "off", 3) && strncasecmp(str, "false", 4);
      }
      ParserState &state;
    };

    struct add_row_key_column {
      add_row_key_column(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String column(str, end-str);
        trim_if(column, is_any_of("'\""));
        state.key_columns.push_back(column);
      }
      ParserState &state;
    };

    struct set_timestamp_column {
      set_timestamp_column(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.timestamp_column = String(str, end-str);
        trim_if(state.timestamp_column, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_row_uniquify_chars {
      set_row_uniquify_chars(ParserState &state) : state(state) { }
      void operator()(int nchars) const {
        state.row_uniquify_chars = nchars;
      }
      ParserState &state;
    };

    struct scan_add_column_family {
      scan_add_column_family(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String column_name(str, end-str);
        trim_if(column_name, is_any_of("'\""));
        state.scan.builder.add_column(column_name.c_str());
      }
      ParserState &state;
    };

    struct scan_set_display_timestamps {
      scan_set_display_timestamps(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scan.display_timestamps=true;
      }
      ParserState &state;
    };

    struct scan_add_row_interval {
      scan_add_row_interval(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        RowInterval &ri = state.scan.current_ri;
        HT_ASSERT(!ri.empty());
        state.scan.builder.add_row_interval(ri.start.c_str(),
            ri.start_inclusive, ri.end.c_str(), ri.end_inclusive);
        ri.clear();
      }
      ParserState &state;
    };

    struct scan_set_cell_row {
      scan_set_cell_row(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scan.current_cell_row = String(str, end-str);
        trim_if(state.scan.current_cell_row, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct scan_set_cell_column {
      scan_set_cell_column(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        CellInterval &ci = state.scan.current_ci;
        String &row = state.scan.current_cell_row;
        String &column = state.scan.current_cell_column;

        column = String(str, end-str);
        trim_if(column, is_any_of("'\""));

        if (state.scan.current_relop) {
          switch (state.scan.current_relop) {
          case RELOP_EQ:
            if (!ci.empty())
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_start(row, column, true);
            ci.set_end(row, column, true);
            break;
          case RELOP_LT:
            if (ci.end_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_end(row, column, false);
            break;
          case RELOP_LE:
            if (ci.end_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_end(row, column, true);
            break;
          case RELOP_GT:
            if (ci.start_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_start(row, column, false);
            break;
          case RELOP_GE:
            if (ci.start_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_start(row, column, true);
            break;
          case RELOP_SW:
            if (!ci.empty())
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad cell expression");
            ci.set_start(row, column, true);
            column.append(1, 0xff);
            ci.set_end(row, column, false);
            break;
          }
          if (!ci.end_set)
            ci.end_row = Key::END_ROW_MARKER;

          state.scan.builder.add_cell_interval(ci.start_row.c_str(),
              ci.start_column.c_str(), ci.start_inclusive, ci.end_row.c_str(),
              ci.end_column.c_str(), ci.end_inclusive);
          ci.clear();
          row.clear();
          column.clear();
          state.scan.current_relop = 0;
        }
      }
      ParserState &state;
    };

    struct scan_set_row {
      scan_set_row(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scan.current_rowkey = String(str, end-str);
        trim_if(state.scan.current_rowkey, is_any_of("'\""));
        if (state.scan.current_relop != 0) {
          switch (state.scan.current_relop) {
          case RELOP_EQ:
            if (!state.scan.current_ri.empty())
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_start(state.scan.current_rowkey, true);
            state.scan.current_ri.set_end(state.scan.current_rowkey, true);
            break;
          case RELOP_LT:
            if (state.scan.current_ri.end_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_end(state.scan.current_rowkey, false);
            break;
          case RELOP_LE:
            if (state.scan.current_ri.end_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_end(state.scan.current_rowkey, true);
            break;
          case RELOP_GT:
            if (state.scan.current_ri.start_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_start(state.scan.current_rowkey, false);
            break;
          case RELOP_GE:
            if (state.scan.current_ri.start_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_start(state.scan.current_rowkey, true);
            break;
          case RELOP_SW:
            if (!state.scan.current_ri.empty())
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad row expressions.");
            state.scan.current_ri.set_start(state.scan.current_rowkey, true);
            str = state.scan.current_rowkey.c_str();
            end = str + state.scan.current_rowkey.length();
            const char *ptr;
            for (ptr = end - 1; ptr > str; --ptr) {
              if (uint8_t(*ptr) < 0xffu) {
                String temp_str(str, ptr - str);
                temp_str.append(1, (*ptr) + 1);
                state.scan.current_ri.set_end(temp_str, false);
                break;
              }
            }
            if (ptr == str) {
              state.scan.current_rowkey.append(4, (char)0xff);
              state.scan.current_ri.set_end(state.scan.current_rowkey, false);
            }
          }
          state.scan.current_rowkey_set = false;
          state.scan.current_relop = 0;
        }
        else {
          state.scan.current_rowkey_set = true;
          state.scan.current_cell_row.clear();
        }
      }
      ParserState &state;
    };

    struct scan_set_max_versions {
      scan_set_max_versions(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        if (state.scan.builder.get().max_versions != 0)
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT MAX_VERSIONS predicate multiply defined.");
        state.scan.builder.set_max_versions(ival);
      }
      ParserState &state;
    };

    struct scan_set_limit {
      scan_set_limit(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        if (state.scan.builder.get().row_limit != 0)
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT LIMIT predicate multiply defined.");
        state.scan.builder.set_row_limit(ival);
      }
      ParserState &state;
    };

    struct scan_set_outfile {
      scan_set_outfile(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        if (state.scan.outfile != "")
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT INTO FILE multiply defined.");
        state.scan.outfile = String(str, end-str);
        trim_if(state.scan.outfile, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct scan_set_year {
      scan_set_year(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_year", ival);
        state.tmval.tm_year = ival - 1900;
      }
      ParserState &state;
    };

    struct scan_set_month {
      scan_set_month(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_month", ival);
        state.tmval.tm_mon = ival-1;
      }
      ParserState &state;
    };

    struct scan_set_day {
      scan_set_day(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_day", ival);
        state.tmval.tm_mday = ival;
      }
      ParserState &state;
    };

    struct scan_set_seconds {
      scan_set_seconds(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_seconds", ival);
        state.tmval.tm_sec = ival;
      }
      ParserState &state;
    };

    struct scan_set_minutes {
      scan_set_minutes(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_minutes", ival);
        state.tmval.tm_min = ival;
      }
      ParserState &state;
    };

    struct scan_set_hours {
      scan_set_hours(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_hours", ival);
        state.tmval.tm_hour = ival;
      }
      ParserState &state;
    };

    struct scan_set_nanoseconds {
      scan_set_nanoseconds(ParserState &state) : state(state) { }
      void operator()(int ival) const {
        HQL_DEBUG_VAL("scan_set_nanoseconds", ival);
        state.nanoseconds = ival;
      }
      ParserState &state;
    };

    struct scan_set_relop {
      scan_set_relop(ParserState &state, int relop)
        : state(state), relop(relop) { }
      void operator()(char const *str, char const *end) const {
        process();
      }
      void operator()(const char c) const {
        process();
      }
      void process() const {
        if (state.scan.current_timestamp_set) {
          switch (relop) {
          case RELOP_EQ:
            if (state.scan.start_time_set || state.scan.end_time_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_time_interval(state.scan.current_timestamp,
                                         state.scan.current_timestamp);
            break;
          case RELOP_GT:
            if (state.scan.end_time_set ||
                (state.scan.start_time_set &&
                 state.scan.start_time() >= state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_end_time(state.scan.current_timestamp);
            break;
          case RELOP_GE:
            if (state.scan.end_time_set ||
                (state.scan.start_time_set &&
                 state.scan.start_time() > state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_end_time(state.scan.current_timestamp + 1);
            break;
          case RELOP_LT:
            if (state.scan.start_time_set ||
                (state.scan.end_time_set &&
                 state.scan.start_time() <= (state.scan.current_timestamp + 1)))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_start_time(state.scan.current_timestamp + 1);
            break;
          case RELOP_LE:
            if (state.scan.start_time_set ||
                (state.scan.end_time_set &&
                 state.scan.end_time() <= state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_start_time(state.scan.current_timestamp);
            break;
          case RELOP_SW:
            HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp operator (=^)");
          }
          state.scan.current_timestamp_set = false;
          state.scan.current_relop = 0;
        }
        else if (state.scan.current_rowkey_set) {
          HT_ASSERT(state.scan.current_rowkey_set);

          switch (relop) {
          case RELOP_EQ:
            state.scan.current_ri.set_start(state.scan.current_rowkey, true);
            state.scan.current_ri.set_end(state.scan.current_rowkey, true);
            break;
          case RELOP_LT:
            state.scan.current_ri.set_start(state.scan.current_rowkey, false);
            break;
          case RELOP_LE:
            state.scan.current_ri.set_start(state.scan.current_rowkey, true);
            break;
          case RELOP_GT:
            state.scan.current_ri.set_end(state.scan.current_rowkey, false);
            break;
          case RELOP_GE:
            state.scan.current_ri.set_end(state.scan.current_rowkey, true);
            break;
          case RELOP_SW:
            HT_THROW(Error::HQL_PARSE_ERROR, "Bad use of operator (=^)");
          }
          state.scan.current_rowkey_set = false;
          state.scan.current_relop = 0;
        }
        else if (state.scan.current_cell_row.size()
                 && state.scan.current_ci.empty()) {
          String &row = state.scan.current_cell_row;
          String &column = state.scan.current_cell_column;

          switch (relop) {
          case RELOP_EQ:
            state.scan.current_ci.set_start(row, column, true);
            state.scan.current_ci.set_end(row, column, true);
            break;
          case RELOP_LT:
            state.scan.current_ci.set_start(row, column, false);
            break;
          case RELOP_LE:
            state.scan.current_ci.set_start(row, column, true);
            break;
          case RELOP_GT:
            state.scan.current_ci.set_end(row, column, false);
            break;
          case RELOP_GE:
            state.scan.current_ci.set_end(row, column, true);
          case RELOP_SW:
            HT_THROW(Error::HQL_PARSE_ERROR, "Bad use of operator (=^)");
          }
          row.clear();
          column.clear();
          state.scan.current_relop = 0;
        }
        else
          state.scan.current_relop = relop;
      }
      ParserState &state;
      int relop;
    };

    struct scan_set_time {
      scan_set_time(ParserState &state) : state(state){ }
      void operator()(char const *str, char const *end) const {
        time_t t = timegm(&state.tmval);
        state.scan.current_timestamp = (int64_t)t * 1000000000LL
                                        + state.nanoseconds;
        if (state.scan.current_relop != 0) {
          switch (state.scan.current_relop) {
          case RELOP_EQ:
            if (state.scan.start_time_set || state.scan.end_time_set)
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_time_interval(state.scan.current_timestamp,
                                         state.scan.current_timestamp + 1);
            break;
          case RELOP_LT:
            if (state.scan.end_time_set ||
                (state.scan.start_time_set &&
                 state.scan.start_time() >= state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_end_time(state.scan.current_timestamp);
            break;
          case RELOP_LE:
            if (state.scan.end_time_set ||
                (state.scan.start_time_set &&
                 state.scan.start_time() > state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_end_time(state.scan.current_timestamp + 1);
            break;
          case RELOP_GT:
            if (state.scan.start_time_set ||
                (state.scan.end_time_set &&
                 state.scan.end_time() <= (state.scan.current_timestamp + 1)))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_start_time(state.scan.current_timestamp + 1);
            break;
          case RELOP_GE:
            if (state.scan.start_time_set ||
                (state.scan.end_time_set &&
                 state.scan.end_time() <= state.scan.current_timestamp))
              HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
            state.scan.set_start_time(state.scan.current_timestamp);
            break;
          case RELOP_SW:
            HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp operator (=^)");
          }
          state.scan.current_relop = 0;
          state.scan.current_timestamp_set = false;
        }
        else
          state.scan.current_timestamp_set = true;

        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      ParserState &state;
    };

    struct scan_set_return_deletes {
      scan_set_return_deletes(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scan.builder.set_return_deletes(true);
      }
      ParserState &state;
    };

    struct set_noescape {
      set_noescape(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.escape = false;
      }
      ParserState &state;
    };

    struct scan_set_keys_only {
      scan_set_keys_only(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scan.keys_only=true;
        state.scan.builder.set_keys_only(true);
      }
      ParserState &state;
    };

    struct set_insert_timestamp {
      set_insert_timestamp(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        time_t t = timegm(&state.tmval);
        if (t == (time_t)-1)
          HT_THROW(Error::HQL_PARSE_ERROR, "INSERT invalid timestamp.");
        state.current_insert_value.timestamp = (uint64_t)t * 1000000000LL
            + state.nanoseconds;
        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      ParserState &state;
    };

    struct set_insert_rowkey {
      set_insert_rowkey(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.current_insert_value.row_key = String(str, end-str);
        trim_if(state.current_insert_value.row_key, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_insert_columnkey {
      set_insert_columnkey(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.current_insert_value.column_key = String(str, end-str);
        trim_if(state.current_insert_value.column_key, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_insert_value {
      set_insert_value(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.current_insert_value.value = String(str, end-str);
        trim_if(state.current_insert_value.value, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct add_insert_value {
      add_insert_value(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        const InsertRecord &rec = state.current_insert_value;
        char *cq;
        Cell cell;

        cell.row_key = rec.row_key.c_str();
        cell.column_family = rec.column_key.c_str();

        if ((cq = (char*)strchr(rec.column_key.c_str(), ':')) != 0) {
          *cq++ = 0;
          cell.column_qualifier = cq;
        }
        cell.timestamp = rec.timestamp;
        cell.value = (uint8_t *)rec.value.c_str();
        cell.value_len = rec.value.length();
        cell.flag = FLAG_INSERT;
        state.inserts.add(cell);
        state.current_insert_value.clear();
      }
      ParserState &state;
    };

    struct delete_column {
      delete_column(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        String column(str, end-str);
        trim_if(column, is_any_of("'\""));
        state.delete_columns.push_back(column);
      }
      void operator()(const char c) const {
        state.delete_all_columns = true;
      }
      ParserState &state;
    };

    struct delete_set_row {
      delete_set_row(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.delete_row = String(str, end-str);
        trim_if(state.delete_row, is_any_of("'\""));
      }
      ParserState &state;
    };

    struct set_delete_timestamp {
      set_delete_timestamp(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        time_t t = timegm(&state.tmval);
        if (t == (time_t)-1)
          HT_THROW(Error::HQL_PARSE_ERROR, String("DELETE invalid timestamp."));
        state.delete_time = (uint64_t)t * 1000000000LL + state.nanoseconds;
        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      ParserState &state;
    };

    struct set_scanner_id {
      set_scanner_id(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.scanner_id = (uint32_t)strtol(str, 0, 10);
      }
      ParserState &state;
    };

    struct set_nokeys {
      set_nokeys(ParserState &state) : state(state) { }
      void operator()(char const *str, char const *end) const {
        state.nokeys=true;
      }
      ParserState &state;
    };


    struct Parser : grammar<Parser> {
      Parser(ParserState &state) : state(state) { }

      template <typename ScannerT>
      struct definition {
        definition(Parser const &self)  {
          keywords =
            "access", "ACCESS", "Access", "GROUP", "group", "Group",
            "from", "FROM", "From", "start_time", "START_TIME", "Start_Time",
            "Start_time", "end_time", "END_TIME", "End_Time", "End_time",
	    "into", "INTO", "Into";

          /**
           * OPERATORS
           */
          chlit<>     ONE('1');
          chlit<>     ZERO('0');
          chlit<>     SINGLEQUOTE('\'');
          chlit<>     DOUBLEQUOTE('"');
          chlit<>     QUESTIONMARK('?');
          chlit<>     PLUS('+');
          chlit<>     MINUS('-');
          chlit<>     STAR('*');
          chlit<>     SLASH('/');
          chlit<>     COMMA(',');
          chlit<>     SEMI(';');
          chlit<>     COLON(':');
          chlit<>     EQUAL('=');
          strlit<>    DOUBLEEQUAL("==");
          chlit<>     LT('<');
          strlit<>    LE("<=");
          strlit<>    GE(">=");
          chlit<>     GT('>');
          strlit<>    SW("=^");
          chlit<>     LPAREN('(');
          chlit<>     RPAREN(')');
          chlit<>     LBRACK('[');
          chlit<>     RBRACK(']');
          chlit<>     POINTER('^');
          chlit<>     DOT('.');
          strlit<>    DOTDOT("..");
          strlit<>    DOUBLEQUESTIONMARK("??");

          /**
           * TOKENS
           */
          typedef inhibit_case<strlit<> > Token;

          Token CREATE       = as_lower_d["create"];
          Token DROP         = as_lower_d["drop"];
          Token ADD          = as_lower_d["add"];
          Token ALTER        = as_lower_d["alter"];
          Token HELP         = as_lower_d["help"];
          Token TABLE        = as_lower_d["table"];
          Token TABLES       = as_lower_d["tables"];
          Token TTL          = as_lower_d["ttl"];
          Token MONTHS       = as_lower_d["months"];
          Token MONTH        = as_lower_d["month"];
          Token WEEKS        = as_lower_d["weeks"];
          Token WEEK         = as_lower_d["week"];
          Token DAYS         = as_lower_d["days"];
          Token DAY          = as_lower_d["day"];
          Token HOURS        = as_lower_d["hours"];
          Token HOUR         = as_lower_d["hour"];
          Token MINUTES      = as_lower_d["minutes"];
          Token MINUTE       = as_lower_d["minute"];
          Token SECONDS      = as_lower_d["seconds"];
          Token SECOND       = as_lower_d["second"];
          Token IN_MEMORY    = as_lower_d["in_memory"];
          Token BLOCKSIZE    = as_lower_d["blocksize"];
          Token ACCESS       = as_lower_d["access"];
          Token GROUP        = as_lower_d["group"];
          Token DESCRIBE     = as_lower_d["describe"];
          Token SHOW         = as_lower_d["show"];
          Token ESC_HELP     = as_lower_d["\\h"];
          Token SELECT       = as_lower_d["select"];
          Token START_TIME   = as_lower_d["start_time"];
          Token END_TIME     = as_lower_d["end_time"];
          Token FROM         = as_lower_d["from"];
          Token WHERE        = as_lower_d["where"];
          Token ROW          = as_lower_d["row"];
          Token CELL         = as_lower_d["cell"];
          Token ROW_KEY_COLUMN = as_lower_d["row_key_column"];
          Token TIMESTAMP_COLUMN = as_lower_d["timestamp_column"];
          Token HEADER_FILE  = as_lower_d["header_file"];
          Token ROW_UNIQUIFY_CHARS = as_lower_d["row_uniquify_chars"];
          Token DUP_KEY_COLS = as_lower_d["dup_key_cols"];
          Token START_ROW    = as_lower_d["start_row"];
          Token END_ROW      = as_lower_d["end_row"];
          Token INCLUSIVE    = as_lower_d["inclusive"];
          Token EXCLUSIVE    = as_lower_d["exclusive"];
          Token MAX_VERSIONS = as_lower_d["max_versions"];
          Token REVS         = as_lower_d["revs"];
          Token LIMIT        = as_lower_d["limit"];
          Token INTO         = as_lower_d["into"];
          Token FILE         = as_lower_d["file"];
          Token LOAD         = as_lower_d["load"];
          Token DATA         = as_lower_d["data"];
          Token INFILE       = as_lower_d["infile"];
          Token TIMESTAMP    = as_lower_d["timestamp"];
          Token INSERT       = as_lower_d["insert"];
          Token DELETE       = as_lower_d["delete"];
          Token VALUES       = as_lower_d["values"];
          Token COMPRESSOR   = as_lower_d["compressor"];
          Token DUMP         = as_lower_d["dump"];
          Token STATS        = as_lower_d["stats"];
          Token STARTS       = as_lower_d["starts"];
          Token WITH         = as_lower_d["with"];
          Token IF           = as_lower_d["if"];
          Token EXISTS       = as_lower_d["exists"];
          Token DISPLAY_TIMESTAMPS = as_lower_d["display_timestamps"];
          Token RETURN_DELETES = as_lower_d["return_deletes"];
          Token KEYS_ONLY    = as_lower_d["keys_only"];
          Token RANGE        = as_lower_d["range"];
          Token UPDATE       = as_lower_d["update"];
          Token SCANNER      = as_lower_d["scanner"];
          Token ON           = as_lower_d["on"];
          Token DESTROY      = as_lower_d["destroy"];
          Token FETCH        = as_lower_d["fetch"];
          Token SCANBLOCK    = as_lower_d["scanblock"];
          Token CLOSE        = as_lower_d["close"];
          Token SHUTDOWN     = as_lower_d["shutdown"];
          Token REPLAY       = as_lower_d["replay"];
          Token START        = as_lower_d["start"];
          Token COMMIT       = as_lower_d["commit"];
          Token LOG          = as_lower_d["log"];
          Token BLOOMFILTER  = as_lower_d["bloomfilter"];
          Token TRUE         = as_lower_d["true"];
          Token FALSE        = as_lower_d["false"];
          Token YES          = as_lower_d["yes"];
          Token NO           = as_lower_d["no"];
          Token OFF          = as_lower_d["off"];
          Token AND          = as_lower_d["and"];
          Token OR           = as_lower_d["or"];
          Token LIKE         = as_lower_d["like"];
          Token NOESCAPE     = as_lower_d["noescape"];
          Token IDS          = as_lower_d["ids"];
          Token NOKEYS       = as_lower_d["nokeys"];

          /**
           * Start grammar definition
           */
          boolean_literal
            = lexeme_d[TRUE | FALSE | YES | NO | ON | OFF | ONE | ZERO]
            ;

          identifier
            = lexeme_d[(alpha_p >> *(alnum_p | '_')) - (keywords)];

          string_literal
            = single_string_literal
            | double_string_literal
            ;

          single_string_literal
            = lexeme_d[SINGLEQUOTE >> +(anychar_p-chlit<>('\''))
                >> SINGLEQUOTE];

          double_string_literal
            = lexeme_d[DOUBLEQUOTE >> +(anychar_p-chlit<>('"'))
                >> DOUBLEQUOTE];

          user_identifier
            = identifier
            | string_literal
            ;

          statement
            = select_statement[set_command(self.state, COMMAND_SELECT)]
            | create_table_statement[set_command(self.state,
                COMMAND_CREATE_TABLE)]
            | describe_table_statement[set_command(self.state,
                COMMAND_DESCRIBE_TABLE)]
            | load_data_statement[set_command(self.state, COMMAND_LOAD_DATA)]
            | show_statement[set_command(self.state, COMMAND_SHOW_CREATE_TABLE)]
            | help_statement[set_help(self.state)]
            | insert_statement[set_command(self.state, COMMAND_INSERT)]
            | delete_statement[set_command(self.state, COMMAND_DELETE)]
            | show_tables_statement[set_command(self.state,
                COMMAND_SHOW_TABLES)]
            | drop_table_statement[set_command(self.state, COMMAND_DROP_TABLE)]
            | alter_table_statement[set_command(self.state, COMMAND_ALTER_TABLE)]

            | load_range_statement[set_command(self.state, COMMAND_LOAD_RANGE)]
            | dump_statement[set_command(self.state, COMMAND_DUMP)]
            | update_statement[set_command(self.state, COMMAND_UPDATE)]
            | create_scanner_statement[set_command(self.state,
                COMMAND_CREATE_SCANNER)]
            | destroy_scanner_statement[set_command(self.state,
                COMMAND_DESTROY_SCANNER)]
            | fetch_scanblock_statement[set_command(self.state,
                COMMAND_FETCH_SCANBLOCK)]
            | close_statement[set_command(self.state, COMMAND_CLOSE)]
            | shutdown_statement[set_command(self.state, COMMAND_SHUTDOWN)]
            | drop_range_statement[set_command(self.state, COMMAND_DROP_RANGE)]
            | replay_start_statement[set_command(self.state,
                COMMAND_REPLAY_BEGIN)]
            | replay_log_statement[set_command(self.state, COMMAND_REPLAY_LOG)]
            | replay_commit_statement[set_command(self.state,
                COMMAND_REPLAY_COMMIT)]
            ;

          drop_range_statement
            = DROP >> RANGE >> range_spec
            ;

          replay_start_statement
            = REPLAY >> START
            ;

          replay_log_statement
            = REPLAY >> LOG >> user_identifier[set_input_file(self.state)]
            ;

          replay_commit_statement
            = REPLAY >> COMMIT
            ;

          close_statement
            = CLOSE
            ;

          shutdown_statement
            = SHUTDOWN
            ;

          fetch_scanblock_statement
            = FETCH >> SCANBLOCK >> !(lexeme_d[(+digit_p)[
                set_scanner_id(self.state)]])
            ;

          destroy_scanner_statement
            = DESTROY >> SCANNER >> !(lexeme_d[(+digit_p)[
                set_scanner_id(self.state)]])
            ;

          create_scanner_statement
            = CREATE >> SCANNER >> ON >> range_spec
              >> !where_clause
              >> *(option_spec)
            ;

          update_statement
            = UPDATE >> user_identifier[set_table_name(self.state)]
              >> user_identifier[set_input_file(self.state)]
            ;

          load_range_statement
            = LOAD >> RANGE >> range_spec >> !(REPLAY[set_replay(self.state)])
            ;

          dump_statement
	    = DUMP >> !(NOKEYS[set_nokeys(self.state)])
		   >> string_literal[set_output_file(self.state)]
            ;

          range_spec
            = user_identifier[set_table_name(self.state)]
            >> LBRACK >> !(user_identifier[set_range_start_row(self.state)])
            >> DOTDOT
            >> (user_identifier | DOUBLEQUESTIONMARK)[
                set_range_end_row(self.state)] >> RBRACK
            ;

          drop_table_statement
            = DROP >> TABLE >> !(IF >> EXISTS[set_if_exists(self.state)])
              >> user_identifier[set_table_name(self.state)]
            ;

          alter_table_statement
            = ALTER >> TABLE >> user_identifier[set_table_name(self.state)]
            >> +(ADD >> add_column_definitions
                | DROP >> drop_column_definitions)
            ;

          show_tables_statement
            = SHOW >> TABLES
            ;

          delete_statement
            = DELETE >> delete_column_clause
              >> FROM >> user_identifier[set_table_name(self.state)]
              >> WHERE >> ROW >> EQUAL >> string_literal[
                  delete_set_row(self.state)]
              >> !(TIMESTAMP >> date_expression[
                  set_delete_timestamp(self.state)])
            ;

          delete_column_clause
            = (STAR[delete_column(self.state)] | (column_name[
                delete_column(self.state)]
              >> *(COMMA >> column_name[delete_column(self.state)])))
            ;

          insert_statement
            = INSERT >> INTO >> identifier[set_table_name(self.state)]
              >> VALUES >> insert_value_list
            ;

          insert_value_list
            = insert_value[add_insert_value(self.state)] >> *(COMMA
              >> insert_value[add_insert_value(self.state)])
            ;

          insert_value
            = LPAREN
              >> *(date_expression[set_insert_timestamp(self.state)] >> COMMA)
              >> string_literal[set_insert_rowkey(self.state)] >> COMMA
              >> string_literal[set_insert_columnkey(self.state)] >> COMMA
              >> string_literal[set_insert_value (self.state)] >> RPAREN
            ;

          show_statement
            = (SHOW >> CREATE >> TABLE >> user_identifier[
                set_table_name(self.state)])
            ;

          help_statement
            = (HELP | ESC_HELP | QUESTIONMARK) >> *anychar_p
            ;

          describe_table_statement
            = DESCRIBE >> TABLE >> !(WITH >> IDS[set_with_ids(self.state)])
                       >> user_identifier[set_table_name(self.state)]
            ;

          create_table_statement
            = CREATE >> TABLE
              >> *(table_option)
              >> user_identifier[set_table_name(self.state)]
              >> *(LIKE >> user_identifier[set_clone_table_name(self.state)])
              >> !(create_definitions)
            ;

          table_option
            = COMPRESSOR >> EQUAL >> string_literal[
                set_table_compressor(self.state)]
            | table_option_in_memory[set_table_in_memory(self.state)]
            | table_option_blocksize
            | max_versions_option
            | ttl_option
            ;

          table_option_in_memory
            = IN_MEMORY
            ;

          table_option_blocksize
            = BLOCKSIZE >> EQUAL >> uint_p[
                set_table_blocksize(self.state)]
            ;

          create_definitions
            = LPAREN >> create_definition
                     >> *(COMMA >> create_definition)
                     >> RPAREN
            ;

          create_definition
            = column_definition
              | access_group_definition
            ;

          add_column_definitions
            = LPAREN >> add_column_definition
                     >> *(COMMA >> add_column_definition)
                     >> RPAREN
            ;

          add_column_definition
            = column_definition
              | access_group_definition
            ;

          drop_column_definitions
            = LPAREN >> drop_column_definition
                     >> *(COMMA >> drop_column_definition)
                     >> RPAREN
            ;

          drop_column_definition
            = column_name[drop_column_family(self.state)]
            ;

          column_name
            = (identifier | string_literal)
            ;

          column_definition
            = column_name[create_column_family(self.state)] >> *(column_option)
            ;

          column_option
            = max_versions_option
            | ttl_option
            ;

          max_versions_option
	    = (MAX_VERSIONS | REVS) >> !EQUAL >> lexeme_d[(+digit_p)[
                set_max_versions(self.state)]]
            ;

          ttl_option
            = TTL >> EQUAL >> duration[set_ttl(self.state)]
            ;

          duration
            = ureal_p >> !(MONTHS | MONTH | WEEKS | WEEK | DAYS | DAY | HOURS |
                HOUR | MINUTES | MINUTE | SECONDS | SECOND)
            ;

          access_group_definition
            = ACCESS >> GROUP
              >> user_identifier[create_access_group(self.state)]
              >> *(access_group_option)
              >> !(LPAREN >> column_name[add_column_family(self.state)]
              >> *(COMMA >> column_name[add_column_family(self.state)])
              >> RPAREN)
            ;

          access_group_option
            = in_memory_option[set_access_group_in_memory(self.state)]
            | blocksize_option
            | COMPRESSOR >> EQUAL >> string_literal[
                set_access_group_compressor(self.state)]
            | bloom_filter_option
            ;

          bloom_filter_option
            = BLOOMFILTER >> EQUAL
              >> string_literal[set_access_group_bloom_filter(self.state)]
            ;

          in_memory_option
            = IN_MEMORY
            ;

          blocksize_option
            = BLOCKSIZE >> EQUAL >> uint_p[
                set_access_group_blocksize(self.state)]
            ;

          select_statement
            = SELECT
              >> ('*' | (user_identifier[scan_add_column_family(self.state)]
              >> *(COMMA >> user_identifier[
                  scan_add_column_family(self.state)])))
              >> FROM >> user_identifier[set_table_name(self.state)]
              >> !where_clause
              >> *(option_spec)
            ;

          where_clause
            = WHERE >> where_predicate >> *(AND >> where_predicate)
            ;

          relop
            = SW[scan_set_relop(self.state, RELOP_SW)]
            | EQUAL[scan_set_relop(self.state, RELOP_EQ)]
            | LE[scan_set_relop(self.state, RELOP_LE)]
            | LT[scan_set_relop(self.state, RELOP_LT)]
            | GE[scan_set_relop(self.state, RELOP_GE)]
            | GT[scan_set_relop(self.state, RELOP_GT)]
            ;

          time_predicate
            = !(date_expression[scan_set_time(self.state)] >> relop) >>
            TIMESTAMP >> relop >> date_expression[scan_set_time(self.state)]
            ;

          row_interval
            = !(string_literal[scan_set_row(self.state)] >> relop) >>
            ROW >> relop >> string_literal[scan_set_row(self.state)]
            ;

          row_predicate
            = row_interval[scan_add_row_interval(self.state)]
            | LPAREN >> row_interval[scan_add_row_interval(self.state)] >>
            *( OR >> row_interval[scan_add_row_interval(self.state)]) >> RPAREN
            ;

          cell_spec
            = string_literal[scan_set_cell_row(self.state)]
              >> COMMA >> string_literal[scan_set_cell_column(self.state)]
            ;

          cell_interval
            = !(cell_spec >> relop) >> CELL >> relop >> cell_spec
            ;

          cell_predicate
            = cell_interval
            | LPAREN >> cell_interval >> *( OR >> cell_interval ) >> RPAREN
            ;

          where_predicate
            = cell_predicate
            | row_predicate
            | time_predicate
            ;

          option_spec
            = MAX_VERSIONS >> EQUAL >> uint_p[scan_set_max_versions(self.state)]
            | REVS >> !EQUAL >> uint_p[scan_set_max_versions(self.state)]
            | LIMIT >> EQUAL >> uint_p[scan_set_limit(self.state)]
            | LIMIT >> uint_p[scan_set_limit(self.state)]
            | INTO >> FILE >> string_literal[scan_set_outfile(self.state)]
            | DISPLAY_TIMESTAMPS[scan_set_display_timestamps(self.state)]
            | RETURN_DELETES[scan_set_return_deletes(self.state)]
            | KEYS_ONLY[scan_set_keys_only(self.state)]
            | NOESCAPE[set_noescape(self.state)]
            ;

          date_expression
            = SINGLEQUOTE >> (datetime | date | time | year) >> SINGLEQUOTE
            | DOUBLEQUOTE >> (datetime | date | time | year) >> DOUBLEQUOTE
            ;

          datetime
            = date >> time
            ;

          uint_parser<unsigned int, 10, 2, 2> uint2_p;
          uint_parser<unsigned int, 10, 4, 4> uint4_p;

          date
            = lexeme_d[limit_d(0u, 9999u)[uint4_p[scan_set_year(self.state)]]
                >> '-'  //  Year
                >>  limit_d(1u, 12u)[uint2_p[scan_set_month(self.state)]]
                >> '-'  //  Month 01..12
                >>  limit_d(1u, 31u)[uint2_p[scan_set_day(self.state)]]
                        //  Day 01..31
                ];

          time
            = lexeme_d[limit_d(0u, 23u)[uint2_p[scan_set_hours(self.state)]]
                >> ':'  //  Hours 00..23
                >>  limit_d(0u, 59u)[uint2_p[scan_set_minutes(self.state)]]
                >> ':'  //  Minutes 00..59
                >>  limit_d(0u, 59u)[uint2_p[scan_set_seconds(self.state)]]
                >>      //  Seconds 00..59
                !(DOT >> uint_p[scan_set_nanoseconds(self.state)])
                ];

          year
            = lexeme_d[limit_d(0u, 9999u)[uint4_p[scan_set_year(self.state)]]]
            ;

          load_data_statement
            = LOAD >> DATA >> INFILE
            >> !(load_data_option >> *(load_data_option))
            >> load_data_input
            >> INTO
            >> (TABLE >> user_identifier[set_table_name(self.state)]
                | FILE >> user_identifier[set_output_file(self.state)])
            ;

          load_data_input
            =  string_literal[set_input_file(self.state)]
            ;

          load_data_option
            = ROW_KEY_COLUMN >> EQUAL >> user_identifier[
                add_row_key_column(self.state)] >> *(PLUS >> user_identifier[
                add_row_key_column(self.state)])
            | TIMESTAMP_COLUMN >> EQUAL >> user_identifier[
                set_timestamp_column(self.state)]
            | HEADER_FILE >> EQUAL >> string_literal[
                set_header_file(self.state)]
            | ROW_UNIQUIFY_CHARS >> EQUAL >> uint_p[
                set_row_uniquify_chars(self.state)]
            | DUP_KEY_COLS >> EQUAL >> boolean_literal[
                set_dup_key_cols(self.state)]
            | NOESCAPE[set_noescape(self.state)]
            ;

          /**
           * End grammar definition
           */

#ifdef BOOST_SPIRIT_DEBUG
          BOOST_SPIRIT_DEBUG_RULE(column_definition);
          BOOST_SPIRIT_DEBUG_RULE(column_name);
          BOOST_SPIRIT_DEBUG_RULE(column_option);
          BOOST_SPIRIT_DEBUG_RULE(create_definition);
          BOOST_SPIRIT_DEBUG_RULE(create_definitions);
          BOOST_SPIRIT_DEBUG_RULE(add_column_definition);
          BOOST_SPIRIT_DEBUG_RULE(add_column_definitions);
          BOOST_SPIRIT_DEBUG_RULE(drop_column_definition);
          BOOST_SPIRIT_DEBUG_RULE(drop_column_definitions);
          BOOST_SPIRIT_DEBUG_RULE(create_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(duration);
          BOOST_SPIRIT_DEBUG_RULE(identifier);
          BOOST_SPIRIT_DEBUG_RULE(user_identifier);
          BOOST_SPIRIT_DEBUG_RULE(max_versions_option);
          BOOST_SPIRIT_DEBUG_RULE(statement);
          BOOST_SPIRIT_DEBUG_RULE(string_literal);
          BOOST_SPIRIT_DEBUG_RULE(single_string_literal);
          BOOST_SPIRIT_DEBUG_RULE(double_string_literal);
          BOOST_SPIRIT_DEBUG_RULE(ttl_option);
          BOOST_SPIRIT_DEBUG_RULE(access_group_definition);
          BOOST_SPIRIT_DEBUG_RULE(access_group_option);
          BOOST_SPIRIT_DEBUG_RULE(bloom_filter_option);
          BOOST_SPIRIT_DEBUG_RULE(in_memory_option);
          BOOST_SPIRIT_DEBUG_RULE(blocksize_option);
          BOOST_SPIRIT_DEBUG_RULE(help_statement);
          BOOST_SPIRIT_DEBUG_RULE(describe_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(show_statement);
          BOOST_SPIRIT_DEBUG_RULE(select_statement);
          BOOST_SPIRIT_DEBUG_RULE(where_clause);
          BOOST_SPIRIT_DEBUG_RULE(where_predicate);
          BOOST_SPIRIT_DEBUG_RULE(time_predicate);
          BOOST_SPIRIT_DEBUG_RULE(cell_interval);
          BOOST_SPIRIT_DEBUG_RULE(cell_predicate);
          BOOST_SPIRIT_DEBUG_RULE(cell_spec);
          BOOST_SPIRIT_DEBUG_RULE(relop);
          BOOST_SPIRIT_DEBUG_RULE(row_interval);
          BOOST_SPIRIT_DEBUG_RULE(row_predicate);
          BOOST_SPIRIT_DEBUG_RULE(option_spec);
          BOOST_SPIRIT_DEBUG_RULE(date_expression);
          BOOST_SPIRIT_DEBUG_RULE(datetime);
          BOOST_SPIRIT_DEBUG_RULE(date);
          BOOST_SPIRIT_DEBUG_RULE(time);
          BOOST_SPIRIT_DEBUG_RULE(year);
          BOOST_SPIRIT_DEBUG_RULE(load_data_statement);
          BOOST_SPIRIT_DEBUG_RULE(load_data_input);
          BOOST_SPIRIT_DEBUG_RULE(load_data_option);
          BOOST_SPIRIT_DEBUG_RULE(insert_statement);
          BOOST_SPIRIT_DEBUG_RULE(insert_value_list);
          BOOST_SPIRIT_DEBUG_RULE(insert_value);
          BOOST_SPIRIT_DEBUG_RULE(delete_statement);
          BOOST_SPIRIT_DEBUG_RULE(delete_column_clause);
          BOOST_SPIRIT_DEBUG_RULE(table_option);
          BOOST_SPIRIT_DEBUG_RULE(show_tables_statement);
          BOOST_SPIRIT_DEBUG_RULE(drop_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(alter_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(load_range_statement);
          BOOST_SPIRIT_DEBUG_RULE(dump_statement);
          BOOST_SPIRIT_DEBUG_RULE(range_spec);
          BOOST_SPIRIT_DEBUG_RULE(update_statement);
          BOOST_SPIRIT_DEBUG_RULE(create_scanner_statement);
          BOOST_SPIRIT_DEBUG_RULE(destroy_scanner_statement);
          BOOST_SPIRIT_DEBUG_RULE(fetch_scanblock_statement);
          BOOST_SPIRIT_DEBUG_RULE(close_statement);
          BOOST_SPIRIT_DEBUG_RULE(shutdown_statement);
          BOOST_SPIRIT_DEBUG_RULE(drop_range_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_start_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_log_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_commit_statement);
#endif
        }

        rule<ScannerT> const&
        start() const { return statement; }

        symbols<> keywords;

        rule<ScannerT> boolean_literal, column_definition, column_name,
          column_option, create_definition, create_definitions,
          add_column_definition, add_column_definitions,
          drop_column_definition, drop_column_definitions,
          create_table_statement, duration, identifier, user_identifier,
          max_versions_option, statement, single_string_literal,
          double_string_literal, string_literal, ttl_option,
          access_group_definition, access_group_option,
          bloom_filter_option, in_memory_option,
          blocksize_option, help_statement, describe_table_statement,
          show_statement, select_statement, where_clause, where_predicate,
          time_predicate, relop, row_interval, row_predicate,
          option_spec, date_expression, datetime, date, time, year,
          load_data_statement, load_data_input, load_data_option, insert_statement,
          insert_value_list, insert_value, delete_statement,
          delete_column_clause, table_option, table_option_in_memory,
          table_option_blocksize, show_tables_statement,
          drop_table_statement, alter_table_statement,load_range_statement,
          dump_statement, range_spec, update_statement, create_scanner_statement,
          destroy_scanner_statement, fetch_scanblock_statement,
          close_statement, shutdown_statement, drop_range_statement,
          replay_start_statement, replay_log_statement,
          replay_commit_statement, cell_interval, cell_predicate,
          cell_spec;
      };

      ParserState &state;
    };
  }
}

#endif // HYPERTABLE_HQLPARSER_H
