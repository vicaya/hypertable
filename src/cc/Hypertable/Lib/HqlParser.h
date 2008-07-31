/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#define display_string(str) cerr << str << endl
#define display_string_with_val(str, val) cerr << str << " val=" << val << endl
#else
#define display_string(str)
#define display_string_with_val(str, val)
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
#include "Schema.h"
#include "ScanSpec.h"


namespace Hypertable {

  namespace HqlParser {

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

    class insert_record {
    public:
      insert_record() : timestamp(0) { }
      void clear() {
        timestamp = 0;
        row_key = "";
        column_key = "";
        value = "";
      }
      uint64_t    timestamp;
      String row_key;
      String column_key;
      String value;
    };

    class hql_interpreter_row_interval {
    public:
      hql_interpreter_row_interval() : start_inclusive(true), start_set(false),
				       end(Key::END_ROW_MARKER),
				       end_inclusive(true), end_set(false) { }
      void clear() {
	start = "";
	end = Key::END_ROW_MARKER;
	start_inclusive = end_inclusive = true;
	start_set = end_set = false;
      }
      bool empty() { return !(start_set || end_set); }

      void set_start(const String &s, bool inclusive) {
	start = s;
	start_inclusive = inclusive;
	start_set = true;
      }
      void set_end(const String &s, bool inclusive) {
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

    class hql_interpreter_scan_state {
    public:
      hql_interpreter_scan_state()
	: limit(0), max_versions(0), display_timestamps(false),
	  return_deletes(false), keys_only(false), current_rowkey_set(false),
	  start_time(BEGINNING_OF_TIME), start_time_set(false),
	  end_time(END_OF_TIME), end_time_set(false),
	  current_timestamp_set(false), current_relop(0) { }
      std::vector<String> columns;
      uint32_t limit;
      uint32_t max_versions;
      String outfile;
      bool display_timestamps;
      bool return_deletes;
      bool keys_only;
      String current_rowkey;
      bool   current_rowkey_set;
      std::vector<hql_interpreter_row_interval> row_intervals;
      hql_interpreter_row_interval current_ri;
      int64_t start_time;
      bool    start_time_set;
      int64_t end_time;
      bool    end_time_set;
      int64_t current_timestamp;
      bool    current_timestamp_set;
      int current_relop;
    };

    class hql_interpreter_state {
    public:
      hql_interpreter_state() : command(0), dupkeycols(false), cf(0), ag(0),
          nanoseconds(0), delete_all_columns(false), delete_time(0),
          if_exists(false), replay(false), scanner_id(-1),
          row_uniquify_chars(0) {
        memset(&tmval, 0, sizeof(tmval));
      }
      int command;
      String table_name;
      String str;
      String output_file;
      String input_file;
      String header_file;
      String table_compressor;
      std::vector<String> key_columns;
      String timestamp_column;
      bool dupkeycols;
      Schema::ColumnFamily *cf;
      Schema::AccessGroup *ag;
      Schema::ColumnFamilyMap cf_map;
      Schema::AccessGroupMap ag_map;
      struct tm tmval;
      uint32_t nanoseconds;
      hql_interpreter_scan_state scan;
      insert_record current_insert_value;
      std::vector<insert_record> inserts;
      std::vector<String> delete_columns;
      bool delete_all_columns;
      String delete_row;
      uint64_t delete_time;
      bool if_exists;
      bool replay;
      String range_start_row;
      String range_end_row;
      int32_t scanner_id;
      int32_t row_uniquify_chars;
    };

    struct set_command {
      set_command(hql_interpreter_state &state_, int cmd)
          : state(state_), command(cmd) { }
      void operator()(char const *, char const *) const {
        display_string("set_command");
        state.command = command;
      }
      hql_interpreter_state &state;
      int command;
    };

    struct set_table_name {
      set_table_name(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_table_name");
        state.table_name = String(str, end-str);
        trim_if(state.table_name, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_range_start_row {
      set_range_start_row(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_range_start_row");
        state.range_start_row = String(str, end-str);
        trim_if(state.range_start_row, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_range_end_row {
      set_range_end_row(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_range_end_row");
        state.range_end_row = String(str, end-str);
        trim_if(state.range_end_row, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_if_exists {
      set_if_exists(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_if_exists");
        state.if_exists = true;
      }
      hql_interpreter_state &state;
    };

    struct set_replay {
      set_replay(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_replay");
        state.replay = true;
      }
      hql_interpreter_state &state;
    };

    struct create_column_family {
      create_column_family(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_column_family");
        state.cf = new Schema::ColumnFamily();
        state.cf->name = String(str, end-str);
        trim_if(state.cf->name, is_any_of("'\""));
        Schema::ColumnFamilyMap::const_iterator iter =
            state.cf_map.find(state.cf->name);
        if (iter != state.cf_map.end())
          HT_THROW(Error::HQL_PARSE_ERROR, String("Column family '") +
                   state.cf->name + " multiply defined.");
        state.cf_map[state.cf->name] = state.cf;
      }
      hql_interpreter_state &state;
    };

    struct set_column_family_max_versions {
      set_column_family_max_versions(hql_interpreter_state &state_)
          : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_column_family_max_versions");
        state.cf->max_versions = (uint32_t)strtol(str, 0, 10);
      }
      hql_interpreter_state &state;
    };

    struct set_ttl {
      set_ttl(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_ttl");
        char *end_ptr;
        double ttl = strtod(str, &end_ptr);
        String unit_str = String(end_ptr, end-end_ptr);
        std::transform(unit_str.begin(), unit_str.end(), unit_str.begin(),
                       ::tolower);
        if (unit_str.find_first_of("month") == 0)
          state.cf->ttl = (time_t)(ttl * 2592000.0);
        else if (unit_str.find_first_of("week") == 0)
          state.cf->ttl = (time_t)(ttl * 604800.0);
        else if (unit_str.find_first_of("day") == 0)
          state.cf->ttl = (time_t)(ttl * 86400.0);
        else if (unit_str.find_first_of("hour") == 0)
          state.cf->ttl = (time_t)(ttl * 3600.0);
        else if (unit_str.find_first_of("minute") == 0)
          state.cf->ttl = (time_t)(ttl * 60.0);
        else
          state.cf->ttl = (time_t)ttl;
      }
      hql_interpreter_state &state;
    };

    struct create_access_group {
      create_access_group(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("create_access_group");
        String name = String(str, end-str);
        trim_if(name, is_any_of("'\""));
        Schema::AccessGroupMap::const_iterator iter = state.ag_map.find(name);
        if (iter != state.ag_map.end())
          state.ag = (*iter).second;
        else {
          state.ag = new Schema::AccessGroup();
          state.ag->name = name;
          state.ag_map[state.ag->name] = state.ag;
        }
      }
      hql_interpreter_state &state;
    };

    struct set_access_group_in_memory {
      set_access_group_in_memory(hql_interpreter_state &state_)
          : state(state_) { }
      void operator()(char const *, char const *) const {
        display_string("set_access_group_in_memory");
        state.ag->in_memory=true;
      }
      hql_interpreter_state &state;
    };

    struct set_access_group_compressor {
      set_access_group_compressor(hql_interpreter_state &state_)
          : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_access_group_compressor");
        state.ag->compressor = String(str, end-str);
        trim_if(state.ag->compressor, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_access_group_blocksize {
      set_access_group_blocksize(hql_interpreter_state &state_)
          : state(state_) { }
      void operator()(const unsigned int &blocksize) const {
        display_string("set_access_group_blocksize");
        state.ag->blocksize = blocksize;
      }
      hql_interpreter_state &state;
    };

    struct add_column_family {
      add_column_family(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("add_column_family");
        String name = String(str, end-str);
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
      hql_interpreter_state &state;
    };

    struct set_table_compressor {
      set_table_compressor(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_table_compressor");
        if (state.table_compressor != "")
          HT_THROW(Error::HQL_PARSE_ERROR, "table compressor multiply defined");
        state.table_compressor = String(str, end-str);
        trim_if(state.table_compressor, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };


    struct set_help {
      set_help(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_help");
        state.command = COMMAND_HELP;
        state.str = String(str, end-str);
        size_t offset = state.str.find_first_of(' ');
        if (offset != String::npos) {
          state.str = state.str.substr(offset+1);
          trim(state.str);
          std::transform(state.str.begin(), state.str.end(), state.str.begin(),
                         ::tolower);
        }
        else
          state.str = "";
      }
      hql_interpreter_state &state;
    };

    struct set_str {
      set_str(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_str");
        state.str = String(str, end-str);
        trim_if(state.str, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_output_file {
      set_output_file(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_output_file");
        state.output_file = String(str, end-str);
        trim_if(state.output_file, is_any_of("'\""));
        FileUtils::expand_tilde(state.output_file);
      }
      hql_interpreter_state &state;
    };

    struct set_input_file {
      set_input_file(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_input_file");
        state.input_file = String(str, end-str);
        trim_if(state.input_file, is_any_of("'\""));
        FileUtils::expand_tilde(state.input_file);
      }
      hql_interpreter_state &state;
    };

    struct set_header_file {
      set_header_file(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_header_file");
        state.header_file = String(str, end-str);
        trim_if(state.header_file, is_any_of("'\""));
        FileUtils::expand_tilde(state.header_file);
      }
      hql_interpreter_state &state;
    };

    struct set_dup_key_cols {
      set_dup_key_cols(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_dup_key_cols");
        state.dupkeycols = *str != '0' && strncasecmp(str, "no", 2)
            && strncasecmp(str, "off", 3) && strncasecmp(str, "false", 4);
      }
      hql_interpreter_state &state;
    };

    struct add_row_key_column {
      add_row_key_column(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        String column = String(str, end-str);
        display_string("add_row_key_column");
        trim_if(column, is_any_of("'\""));
        state.key_columns.push_back(column);
      }
      hql_interpreter_state &state;
    };

    struct set_timestamp_column {
      set_timestamp_column(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_timestamp_column");
        state.timestamp_column = String(str, end-str);
        trim_if(state.timestamp_column, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_row_uniquify_chars {
      set_row_uniquify_chars(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &nchars) const {
        display_string("set_row_uniquify_chars");
        state.row_uniquify_chars = nchars;
      }
      hql_interpreter_state &state;
    };

    struct scan_add_column_family {
      scan_add_column_family(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        String column_name = String(str, end-str);
        display_string("scan_add_column_family");
        trim_if(column_name, is_any_of("'\""));
        state.scan.columns.push_back(column_name);
      }
      hql_interpreter_state &state;
    };

    struct scan_set_display_timestamps {
      scan_set_display_timestamps(hql_interpreter_state &state_)
          : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_display_timestamps");
        state.scan.display_timestamps=true;
      }
      hql_interpreter_state &state;
    };

    struct scan_add_row_interval {
      scan_add_row_interval(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_add_row_interval");
	HT_EXPECT(!state.scan.current_ri.empty(), Error::FAILED_EXPECTATION);
	state.scan.row_intervals.push_back(state.scan.current_ri);
	state.scan.current_ri.clear();
      }
      hql_interpreter_state &state;
    };

    struct scan_set_row {
      scan_set_row(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_row");
	state.scan.current_rowkey = String(str, end-str);
        trim_if(state.scan.current_rowkey, is_any_of("'\""));
	if (state.scan.current_relop != 0) {
	  if (state.scan.current_relop == RELOP_EQ) {
	    if (!state.scan.current_ri.empty())
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_start(state.scan.current_rowkey, true);
	    state.scan.current_ri.set_end(state.scan.current_rowkey, true);
	  }
	  else if (state.scan.current_relop == RELOP_LT) {
	    if (state.scan.current_ri.end_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_end(state.scan.current_rowkey, false);
	  }
	  else if (state.scan.current_relop == RELOP_LE) {
	    if (state.scan.current_ri.end_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_end(state.scan.current_rowkey, true);
	  }
	  else if (state.scan.current_relop == RELOP_GT) {
	    if (state.scan.current_ri.start_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_start(state.scan.current_rowkey, false);
	  }
	  else if (state.scan.current_relop == RELOP_GE) {
	    if (state.scan.current_ri.start_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_start(state.scan.current_rowkey, true);
	  }
	  else if (state.scan.current_relop == RELOP_SW) {
	    if (!state.scan.current_ri.empty())
	      HT_THROW(Error::HQL_PARSE_ERROR, "Incompatible row expressions.");
	    state.scan.current_ri.set_start(state.scan.current_rowkey, true);
	    str = state.scan.current_rowkey.c_str();
	    end = str + state.scan.current_rowkey.length();
	    const char *ptr;
	    for (ptr=end-1; ptr>str; --ptr) {
	      if (*ptr < (char)0xff) {
		String temp_str = String(str, ptr-str);
		temp_str.append(1, (*ptr)+1);
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
	else
	  state.scan.current_rowkey_set = true;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_max_versions {
      scan_set_max_versions(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string("scan_set_max_versions");
        if (state.scan.max_versions != 0)
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT MAX_VERSIONS predicate multiply defined.");
        state.scan.max_versions = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_limit {
      scan_set_limit(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string("scan_set_limit");
        if (state.scan.limit != 0)
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT LIMIT predicate multiply defined.");
        state.scan.limit = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_outfile {
      scan_set_outfile(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_outfile");
        if (state.scan.outfile != "")
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "SELECT INTO FILE multiply defined.");
        state.scan.outfile = String(str, end-str);
      }
      hql_interpreter_state &state;
    };

    struct scan_set_year {
      scan_set_year(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_year", ival);
        state.tmval.tm_year = ival - 1900;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_month {
      scan_set_month(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_month", ival);
        state.tmval.tm_mon = ival-1;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_day {
      scan_set_day(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_day", ival);
        state.tmval.tm_mday = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_seconds {
      scan_set_seconds(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_seconds", ival);
        state.tmval.tm_sec = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_minutes {
      scan_set_minutes(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_minutes", ival);
        state.tmval.tm_min = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_hours {
      scan_set_hours(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_hours", ival);
        state.tmval.tm_hour = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_nanoseconds {
      scan_set_nanoseconds(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const {
        display_string_with_val("scan_set_nanoseconds", ival);
        state.nanoseconds = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_relop {
      scan_set_relop(hql_interpreter_state &state_, int relop_)
	: state(state_), relop(relop_) { }
      void operator()(char const *str, char const *end) const {
	process();
      }
      void operator()(const char c) const {
	process();
      }
      void process() const {
        display_string("scan_set_relop");
	if (state.scan.current_timestamp_set) {
	  if (relop == RELOP_EQ) {
	    if (state.scan.start_time_set || state.scan.end_time_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp;
	    state.scan.start_time_set = true;
	    state.scan.end_time = state.scan.current_timestamp;
	    state.scan.end_time_set = true;
	  }
	  else if (relop == RELOP_GT) {
	    if (state.scan.end_time_set ||
		(state.scan.start_time_set &&
		 state.scan.start_time >= state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.end_time = state.scan.current_timestamp;
	    state.scan.end_time_set = true;
	  }
	  else if (relop == RELOP_GE) {
	    if (state.scan.end_time_set ||
		(state.scan.start_time_set &&
		 state.scan.start_time > state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.end_time = state.scan.current_timestamp+1;
	    state.scan.end_time_set = true;
	  }
	  else if (relop == RELOP_LT) {
	    if (state.scan.start_time_set ||
		(state.scan.end_time_set &&
		 state.scan.end_time <= (state.scan.current_timestamp+1)))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp+1;
	    state.scan.start_time_set = true;
	  }
	  else if (relop == RELOP_LE) {
	    if (state.scan.start_time_set ||
		(state.scan.end_time_set &&
		 state.scan.end_time <= state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp;
	    state.scan.start_time_set = true;
	  }
	  else if (relop == RELOP_SW) {
	    HT_THROW(Error::HQL_PARSE_ERROR,
		     "Invalid timestamp operator (=^)");
	  }
	  state.scan.current_timestamp_set = false;
	  state.scan.current_relop = 0;
	}
	else if (state.scan.current_rowkey_set) {
	  HT_EXPECT(state.scan.current_rowkey_set, Error::FAILED_EXPECTATION);
	  if (relop == RELOP_EQ) {
	    state.scan.current_ri.set_start(state.scan.current_rowkey, true);
	    state.scan.current_ri.set_end(state.scan.current_rowkey, true);
	  }
	  else if (relop == RELOP_LT)
	    state.scan.current_ri.set_start(state.scan.current_rowkey, false);
	  else if (relop == RELOP_LE)
	    state.scan.current_ri.set_start(state.scan.current_rowkey, true);
	  else if (relop == RELOP_GT)
	    state.scan.current_ri.set_end(state.scan.current_rowkey, false);
	  else if (relop == RELOP_GE)
	    state.scan.current_ri.set_end(state.scan.current_rowkey, true);
	  else if (relop == RELOP_SW) {
	    HT_THROW(Error::HQL_PARSE_ERROR,
		     "Bad use of starts with operator (=^)");
	  }
	  state.scan.current_rowkey_set = false;
	  state.scan.current_relop = 0;
	}
	else
	  state.scan.current_relop = relop;
      }
      hql_interpreter_state &state;
      int relop;
    };
      
    struct scan_set_time {
      scan_set_time(hql_interpreter_state &state_) : state(state_){ }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_time");
        time_t t = timegm(&state.tmval);
	state.scan.current_timestamp = (int64_t)t * 1000000000LL + state.nanoseconds;
	if (state.scan.current_relop != 0) {
	  if (state.scan.current_relop == RELOP_EQ) {
	    if (state.scan.start_time_set || state.scan.end_time_set)
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp;
	    state.scan.start_time_set = true;
	    state.scan.end_time = state.scan.current_timestamp+1;
	    state.scan.end_time_set = true;
	  }
	  else if (state.scan.current_relop == RELOP_LT) {
	    if (state.scan.end_time_set ||
		(state.scan.start_time_set &&
		 state.scan.start_time >= state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.end_time = state.scan.current_timestamp;
	    state.scan.end_time_set = true;
	  }
	  else if (state.scan.current_relop == RELOP_LE) {
	    if (state.scan.end_time_set ||
		(state.scan.start_time_set &&
		 state.scan.start_time > state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.end_time = state.scan.current_timestamp+1;
	    state.scan.end_time_set = true;
	  }
	  else if (state.scan.current_relop == RELOP_GT) {
	    if (state.scan.start_time_set ||
		(state.scan.end_time_set &&
		 state.scan.end_time <= (state.scan.current_timestamp+1)))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp+1;
	    state.scan.start_time_set = true;
	  }
	  else if (state.scan.current_relop == RELOP_GE) {
	    if (state.scan.start_time_set ||
		(state.scan.end_time_set &&
		 state.scan.end_time <= state.scan.current_timestamp))
	      HT_THROW(Error::HQL_PARSE_ERROR, "Bad timestamp expression");
	    state.scan.start_time = state.scan.current_timestamp;
	    state.scan.start_time_set = true;
	  }
	  else if (state.scan.current_relop == RELOP_SW) {
	    HT_THROW(Error::HQL_PARSE_ERROR,
		     "Invalid timestamp operator (=^)");
	  }
	  state.scan.current_relop = 0;
	  state.scan.current_timestamp_set = false;
	}
	else
	  state.scan.current_timestamp_set = true;
        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_return_deletes {
      scan_set_return_deletes(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_return_deletes");
        state.scan.return_deletes=true;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_keys_only {
      scan_set_keys_only(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("scan_set_keys_only");
        state.scan.keys_only=true;
      }
      hql_interpreter_state &state;
    };

    struct set_insert_timestamp {
      set_insert_timestamp(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_insert_timestamp");
        time_t t = timegm(&state.tmval);
        if (t == (time_t)-1)
          HT_THROW(Error::HQL_PARSE_ERROR, "INSERT invalid timestamp.");
        state.current_insert_value.timestamp = (uint64_t)t * 1000000000LL
            + state.nanoseconds;
        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
    };

    struct set_insert_rowkey {
      set_insert_rowkey(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_insert_rowkey");
        state.current_insert_value.row_key = String(str, end-str);
        trim_if(state.current_insert_value.row_key, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_insert_columnkey {
      set_insert_columnkey(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_insert_columnkey");
        state.current_insert_value.column_key = String(str, end-str);
        trim_if(state.current_insert_value.column_key, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_insert_value {
      set_insert_value(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_insert_value");
        state.current_insert_value.value = String(str, end-str);
        trim_if(state.current_insert_value.value, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct add_insert_value {
      add_insert_value(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("add_insert_value");
        state.inserts.push_back(state.current_insert_value);
        state.current_insert_value.clear();
      }
      hql_interpreter_state &state;
    };

    struct delete_column {
      delete_column(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        String column = String(str, end-str);
        display_string("delete_column");
        trim_if(column, is_any_of("'\""));
        state.delete_columns.push_back(column);
      }
      void operator()(const char c) const {
        display_string("delete_column *");
        state.delete_all_columns = true;
      }
      hql_interpreter_state &state;
    };

    struct delete_set_row {
      delete_set_row(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("delete_set_row");
        state.delete_row = String(str, end-str);
        trim_if(state.delete_row, is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_delete_timestamp {
      set_delete_timestamp(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_delete_timestamp");
        time_t t = timegm(&state.tmval);
        if (t == (time_t)-1)
          HT_THROW(Error::HQL_PARSE_ERROR, String("DELETE invalid timestamp."));
        state.delete_time = (uint64_t)t * 1000000000LL + state.nanoseconds;
        memset(&state.tmval, 0, sizeof(state.tmval));
        state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
    };

    struct set_scanner_id {
      set_scanner_id(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const {
        display_string("set_scanner_id");
        state.scanner_id = (uint32_t)strtol(str, 0, 10);
      }
      hql_interpreter_state &state;
    };

    struct hql_interpreter : public grammar<hql_interpreter> {
      hql_interpreter(hql_interpreter_state &state_) : state(state_) { }

      template <typename Scanner>
      struct definition {

        definition(hql_interpreter const &self)  {
#ifdef BOOST_SPIRIT_DEBUG
          debug(); // define the debug names
#endif

          keywords =
            "access", "ACCESS", "Access", "GROUP", "group", "Group",
            "from", "FROM", "From", "start_time", "START_TIME", "Start_Time",
            "Start_time", "end_time", "END_TIME", "End_Time", "End_time";

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
          Token SHUTDOWN     = as_lower_d["shutdown"];
          Token REPLAY       = as_lower_d["replay"];
          Token START        = as_lower_d["start"];
          Token COMMIT       = as_lower_d["commit"];
          Token LOG          = as_lower_d["log"];
          Token TRUE         = as_lower_d["true"];
          Token FALSE        = as_lower_d["false"];
          Token YES          = as_lower_d["yes"];
          Token NO           = as_lower_d["no"];
          Token OFF          = as_lower_d["off"];
          Token AND          = as_lower_d["and"];
          Token OR           = as_lower_d["or"];

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
            | load_range_statement[set_command(self.state, COMMAND_LOAD_RANGE)]
            | update_statement[set_command(self.state, COMMAND_UPDATE)]
            | create_scanner_statement[set_command(self.state,
                COMMAND_CREATE_SCANNER)]
            | destroy_scanner_statement[set_command(self.state,
                COMMAND_DESTROY_SCANNER)]
            | fetch_scanblock_statement[set_command(self.state,
                COMMAND_FETCH_SCANBLOCK)]
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
            = DESCRIBE >> TABLE >> user_identifier[set_table_name(self.state)]
            ;

          create_table_statement
            =  CREATE >> TABLE
                      >> *(table_option)
                      >> user_identifier[set_table_name(self.state)]
                      >> !(create_definitions)
            ;

          table_option
            = COMPRESSOR >> EQUAL >> string_literal[
                set_table_compressor(self.state)]
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
            = (MAX_VERSIONS | REVS) >> EQUAL >> lexeme_d[(+digit_p)[
                set_column_family_max_versions(self.state)]]
            ;

          ttl_option
            = TTL >> EQUAL >> duration[set_ttl(self.state)]
            ;

          duration
            = ureal_p >> (MONTHS | MONTH | WEEKS | WEEK | DAYS | DAY | HOURS |
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

          where_predicate
	    = row_predicate
	    | time_predicate
	    ;

          option_spec
            = MAX_VERSIONS >> EQUAL >> uint_p[scan_set_max_versions(self.state)]
            | REVS >> EQUAL >> uint_p[scan_set_max_versions(self.state)]
            | LIMIT >> EQUAL >> uint_p[scan_set_limit(self.state)]
            | INTO >> FILE >> string_literal[scan_set_outfile(self.state)]
            | DISPLAY_TIMESTAMPS[scan_set_display_timestamps(self.state)]
            | RETURN_DELETES[scan_set_return_deletes(self.state)]
            | KEYS_ONLY[scan_set_keys_only(self.state)]
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
            >> string_literal[set_input_file(self.state)]
            >> INTO
            >> (TABLE >> user_identifier[set_table_name(self.state)]
                | FILE >> user_identifier[set_output_file(self.state)])
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
            ;

          /**
           * End grammar definition
           */

        }

#ifdef BOOST_SPIRIT_DEBUG
        void debug() {
          BOOST_SPIRIT_DEBUG_RULE(column_definition);
          BOOST_SPIRIT_DEBUG_RULE(column_name);
          BOOST_SPIRIT_DEBUG_RULE(column_option);
          BOOST_SPIRIT_DEBUG_RULE(create_definition);
          BOOST_SPIRIT_DEBUG_RULE(create_definitions);
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
          BOOST_SPIRIT_DEBUG_RULE(in_memory_option);
          BOOST_SPIRIT_DEBUG_RULE(blocksize_option);
          BOOST_SPIRIT_DEBUG_RULE(help_statement);
          BOOST_SPIRIT_DEBUG_RULE(describe_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(show_statement);
          BOOST_SPIRIT_DEBUG_RULE(select_statement);
          BOOST_SPIRIT_DEBUG_RULE(where_clause);
          BOOST_SPIRIT_DEBUG_RULE(where_predicate);
          BOOST_SPIRIT_DEBUG_RULE(time_predicate);
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
          BOOST_SPIRIT_DEBUG_RULE(load_data_option);
          BOOST_SPIRIT_DEBUG_RULE(insert_statement);
          BOOST_SPIRIT_DEBUG_RULE(insert_value_list);
          BOOST_SPIRIT_DEBUG_RULE(insert_value);
          BOOST_SPIRIT_DEBUG_RULE(delete_statement);
          BOOST_SPIRIT_DEBUG_RULE(delete_column_clause);
          BOOST_SPIRIT_DEBUG_RULE(table_option);
          BOOST_SPIRIT_DEBUG_RULE(show_tables_statement);
          BOOST_SPIRIT_DEBUG_RULE(drop_table_statement);
          BOOST_SPIRIT_DEBUG_RULE(load_range_statement);
          BOOST_SPIRIT_DEBUG_RULE(range_spec);
          BOOST_SPIRIT_DEBUG_RULE(update_statement);
          BOOST_SPIRIT_DEBUG_RULE(create_scanner_statement);
          BOOST_SPIRIT_DEBUG_RULE(destroy_scanner_statement);
          BOOST_SPIRIT_DEBUG_RULE(fetch_scanblock_statement);
          BOOST_SPIRIT_DEBUG_RULE(shutdown_statement);
          BOOST_SPIRIT_DEBUG_RULE(drop_range_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_start_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_log_statement);
          BOOST_SPIRIT_DEBUG_RULE(replay_commit_statement);
        }
#endif

        rule<Scanner> const&
        start() const { return statement; }

        symbols<> keywords;

        rule<Scanner> boolean_literal, column_definition, column_name,
          column_option, create_definition, create_definitions,
          create_table_statement, duration, identifier, user_identifier,
          max_versions_option, statement, single_string_literal,
          double_string_literal, string_literal, ttl_option,
          access_group_definition, access_group_option, in_memory_option,
          blocksize_option, help_statement, describe_table_statement,
          show_statement, select_statement, where_clause, where_predicate,
          time_predicate, relop, row_interval, row_predicate,
          option_spec, date_expression, datetime, date, time, year,
          load_data_statement, load_data_option, insert_statement,
          insert_value_list, insert_value, delete_statement,
          delete_column_clause, table_option, show_tables_statement,
          drop_table_statement, load_range_statement, range_spec,
          update_statement, create_scanner_statement, destroy_scanner_statement,
          fetch_scanblock_statement, shutdown_statement, drop_range_statement,
          replay_start_statement, replay_log_statement, replay_commit_statement;
        };

      hql_interpreter_state &state;
    };
  }
}

#endif // HYPERTABLE_HQLPARSER_H
