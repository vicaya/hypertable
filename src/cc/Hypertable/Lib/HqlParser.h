/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

extern "C" {
#include <limits.h>
#include <time.h>
}

#include "Schema.h"

using namespace std;
using namespace boost::spirit;


namespace Hypertable {

  namespace HqlParser {

    const int COMMAND_HELP              = 1;
    const int COMMAND_CREATE_TABLE      = 2;
    const int COMMAND_DESCRIBE_TABLE    = 3;
    const int COMMAND_SHOW_CREATE_TABLE = 4;
    const int COMMAND_SELECT            = 5;
    const int COMMAND_LOAD_DATA         = 6;
    const int COMMAND_INSERT            = 7;
    const int COMMAND_DELETE            = 8;
    const int COMMAND_SHOW_TABLES       = 9;
    const int COMMAND_DROP_TABLE        = 10;

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
      std::string row_key;
      std::string column_key;
      std::string value;
    };

    class hql_interpreter_scan_state {
    public:
      hql_interpreter_scan_state() : start_row_inclusive(true), end_row_inclusive(true), limit(0), max_versions(0), start_time(0), end_time(0), display_timestamps(false), return_deletes(false), keys_only(false) { }
      std::vector<std::string> columns;
      std::string row;
      std::string start_row;
      bool start_row_inclusive;
      std::string end_row;
      bool end_row_inclusive;
      uint32_t limit;
      uint32_t max_versions;
      uint64_t start_time;
      uint64_t end_time;
      std::string outfile;
      bool display_timestamps;
      bool return_deletes;
      bool keys_only;
    };
   
    class hql_interpreter_state {
    public:
      hql_interpreter_state() : command(0), cf(0), ag(0), nanoseconds(0), delete_all_columns(false), delete_time(0), if_exists(false) {
	memset(&tmval, 0, sizeof(tmval));
      }
      int command;
      std::string table_name;
      std::string str;
      std::string output_file;
      std::string table_compressor;
      std::string row_key_column;
      std::string timestamp_column;
      Schema::ColumnFamily *cf;
      Schema::AccessGroup *ag;
      Schema::ColumnFamilyMapT cf_map;
      Schema::AccessGroupMapT ag_map;
      struct tm tmval;
      uint32_t nanoseconds;
      hql_interpreter_scan_state scan;
      insert_record current_insert_value;
      std::vector<insert_record> inserts;
      std::vector<std::string> delete_columns;
      bool delete_all_columns;
      std::string delete_row;
      uint64_t delete_time;
      bool if_exists;
    };

    struct set_command {
      set_command(hql_interpreter_state &state_, int cmd) : state(state_), command(cmd) { }
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
	state.table_name = std::string(str, end-str);
	boost::trim_if(state.table_name, boost::is_any_of("'\""));
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

    struct create_column_family {
      create_column_family(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_column_family");
	state.cf = new Schema::ColumnFamily();
	state.cf->name = std::string(str, end-str);
	boost::trim_if(state.cf->name, boost::is_any_of("'\""));
	Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.find(state.cf->name);
	if (iter != state.cf_map.end())
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Column family '") + state.cf->name + " multiply defined.");
	state.cf_map[state.cf->name] = state.cf;
      }
      hql_interpreter_state &state;
    };

    struct set_column_family_max_versions {
      set_column_family_max_versions(hql_interpreter_state &state_) : state(state_) { }
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
	std::string unit_str = std::string(end_ptr, end-end_ptr);
	std::transform(unit_str.begin(), unit_str.end(), unit_str.begin(), ::tolower );
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
	std::string name = std::string(str, end-str);
	boost::trim_if(name, boost::is_any_of("'\""));
	Schema::AccessGroupMapT::const_iterator iter = state.ag_map.find(name);
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
      set_access_group_in_memory(hql_interpreter_state &state_) : state(state_) {  }
      void operator()(char const *, char const *) const { 
	display_string("set_access_group_in_memory");
	state.ag->in_memory=true;
      }
      hql_interpreter_state &state;
    };

    struct set_access_group_compressor {
      set_access_group_compressor(hql_interpreter_state &state_) : state(state_) {  }
      void operator()(char const *str, char const *end) const { 
	display_string("set_access_group_compressor");
	state.ag->compressor = std::string(str, end-str);
	boost::trim_if(state.ag->compressor, boost::is_any_of("'\""));	
      }
      hql_interpreter_state &state;
    };

    struct set_access_group_blocksize {
      set_access_group_blocksize(hql_interpreter_state &state_) : state(state_) { }
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
	std::string name = std::string(str, end-str);
	boost::trim_if(name, boost::is_any_of("'\""));
	Schema::ColumnFamilyMapT::const_iterator iter = state.cf_map.find(name);
	if (iter == state.cf_map.end())
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Access Group '") + state.ag->name + "' includes unknown column family '" + name + "'");
	if ((*iter).second->ag != "" && (*iter).second->ag != state.ag->name)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("Column family '") + name + "' can belong to only one access group");
	(*iter).second->ag = state.ag->name;
      }
      hql_interpreter_state &state;
    };

    struct set_table_compressor {
      set_table_compressor(hql_interpreter_state &state_) : state(state_) {  }
      void operator()(char const *str, char const *end) const { 
	display_string("set_table_compressor");
	if (state.table_compressor != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("table compressor multiply defined"));
	state.table_compressor = std::string(str, end-str);
	boost::trim_if(state.table_compressor, boost::is_any_of("'\""));	
      }
      hql_interpreter_state &state;
    };


    struct set_help {
      set_help(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_help");
	state.command = COMMAND_HELP;
	state.str = std::string(str, end-str);
	size_t offset = state.str.find_first_of(' ');
	if (offset != std::string::npos) {
	  state.str = state.str.substr(offset+1);
	  boost::trim(state.str);
	  std::transform( state.str.begin(), state.str.end(), state.str.begin(), ::tolower );
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
	state.str = std::string(str, end-str);
	trim_if(state.str, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_output_file {
      set_output_file(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_output_file");
	state.output_file = std::string(str, end-str);
	trim_if(state.output_file, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_row_key_column {
      set_row_key_column(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_row_key_column");
	state.row_key_column = std::string(str, end-str);
	trim_if(state.row_key_column, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_timestamp_column {
      set_timestamp_column(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_timestamp_column");
	state.timestamp_column = std::string(str, end-str);
	trim_if(state.timestamp_column, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct scan_add_column_family {
      scan_add_column_family(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	std::string column_name = std::string(str, end-str);
	display_string("scan_add_column_family");
	trim_if(column_name, boost::is_any_of("'\""));
	state.scan.columns.push_back(column_name);
      }
      hql_interpreter_state &state;
    };

    struct scan_set_display_timestamps {
      scan_set_display_timestamps(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_display_timestamps");
	state.scan.display_timestamps=true;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_row {
      scan_set_row(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_row");
	if (state.scan.row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT ROW predicate multiply defined."));
	else if (state.scan.start_row != "" || state.scan.end_row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT conflicting row specifications."));
	state.scan.row = std::string(str, end-str);
	trim_if(state.scan.row, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct scan_set_row_prefix {
      scan_set_row_prefix(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_row_prefix");
	if (state.scan.row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT ROW predicate multiply defined."));
	else if (state.scan.start_row != "" || state.scan.end_row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT conflicting row specifications."));
	state.scan.start_row = std::string(str, end-str);
	trim_if(state.scan.start_row, boost::is_any_of("'\""));
	state.scan.start_row_inclusive = true;
	str = state.scan.start_row.c_str();
	end = str + state.scan.start_row.length();
	const char *ptr;
	for (ptr=end-1; ptr>str; --ptr) {
	  if (*ptr < (char)0xff) {
	    state.scan.end_row = std::string(str, ptr-str);
	    state.scan.end_row.append(1, (*ptr)+1);
	    ptr++;
	    if (ptr < end)
	      state.scan.end_row += std::string(ptr, end-ptr);
	    break;
	  }
	}
	if (ptr == str) {
	  state.scan.end_row  = state.scan.start_row;
	  state.scan.end_row.append(1, (char)0xff);
	}
	state.scan.end_row_inclusive = false;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_start_row {
      scan_set_start_row(hql_interpreter_state &state_, bool inclusive_) : state(state_), inclusive(inclusive_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_start_row");
	if (state.scan.row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT conflicting row specifications."));
	if (state.scan.start_row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT row range lower bound specified more than once."));
	state.scan.start_row = std::string(str, end-str);
	trim_if(state.scan.start_row, boost::is_any_of("'\""));
	state.scan.start_row_inclusive = inclusive;
      }
      hql_interpreter_state &state;
      bool inclusive;
    };

    struct scan_set_end_row {
      scan_set_end_row(hql_interpreter_state &state_, bool inclusive_) : state(state_), inclusive(inclusive_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_end_row");
	if (state.scan.row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT conflicting row specifications."));
	if (state.scan.end_row != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT row range upper bound specified more than once."));
	state.scan.end_row = std::string(str, end-str);
	trim_if(state.scan.end_row, boost::is_any_of("'\""));
	state.scan.end_row_inclusive = inclusive;
      }
      hql_interpreter_state &state;
      bool inclusive;
    };

    struct scan_set_max_versions {
      scan_set_max_versions(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const { 
	display_string("scan_set_max_versions");
	if (state.scan.max_versions != 0)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT MAX_VERSIONS predicate multiply defined."));
	state.scan.max_versions = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_limit {
      scan_set_limit(hql_interpreter_state &state_) : state(state_) { }
      void operator()(const unsigned int &ival) const { 
	display_string("scan_set_limit");
	if (state.scan.limit != 0)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT LIMIT predicate multiply defined."));
	state.scan.limit = ival;
      }
      hql_interpreter_state &state;
    };

    struct scan_set_outfile {
      scan_set_outfile(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_outfile");
	if (state.scan.outfile != "")
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT INTO FILE multiply defined."));
	state.scan.outfile = std::string(str, end-str);
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


    struct scan_set_start_time {
      scan_set_start_time(hql_interpreter_state &state_, bool inclusive_) : state(state_), inclusive(inclusive_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_start_time");
	if (state.scan.start_time != 0)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT multiple start time predicates."));
	time_t t = timegm(&state.tmval);
	if (t == (time_t)-1)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT invalid start time."));
	state.scan.start_time = (uint64_t)t * 1000000000LL + state.nanoseconds;
	if (!inclusive)
	  state.scan.start_time++;
	memset(&state.tmval, 0, sizeof(state.tmval));
	state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
      bool inclusive;
    };

    struct scan_set_end_time {
      scan_set_end_time(hql_interpreter_state &state_, bool inclusive_) : state(state_), inclusive(inclusive_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("scan_set_end_time");
	if (state.scan.end_time != 0)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT multiple end time predicates."));
	time_t t = timegm(&state.tmval);
	if (t == (time_t)-1)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("SELECT invalid end time."));
	state.scan.end_time = (uint64_t)t * 1000000000LL + state.nanoseconds;
	memset(&state.tmval, 0, sizeof(state.tmval));
	state.nanoseconds = 0;
	if (inclusive && state.scan.end_time != 0)
	  state.scan.end_time--;
      }
      hql_interpreter_state &state;
      bool inclusive;
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
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("INSERT invalid timestamp."));
	state.current_insert_value.timestamp = (uint64_t)t * 1000000000LL + state.nanoseconds;
	memset(&state.tmval, 0, sizeof(state.tmval));
	state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
    };

    struct set_insert_rowkey {
      set_insert_rowkey(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_insert_rowkey");
	state.current_insert_value.row_key = std::string(str, end-str);
	boost::trim_if(state.current_insert_value.row_key, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_insert_columnkey {
      set_insert_columnkey(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_insert_columnkey");
	state.current_insert_value.column_key = std::string(str, end-str);
	boost::trim_if(state.current_insert_value.column_key, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_insert_value {
      set_insert_value(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_insert_value");
	state.current_insert_value.value = std::string(str, end-str);
	boost::trim_if(state.current_insert_value.value, boost::is_any_of("'\""));
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
	std::string column = std::string(str, end-str);
	display_string("delete_column");
	boost::trim_if(column, boost::is_any_of("'\""));
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
	state.delete_row = std::string(str, end-str);
	trim_if(state.delete_row, boost::is_any_of("'\""));
      }
      hql_interpreter_state &state;
    };

    struct set_delete_timestamp {
      set_delete_timestamp(hql_interpreter_state &state_) : state(state_) { }
      void operator()(char const *str, char const *end) const { 
	display_string("set_delete_timestamp");
	time_t t = timegm(&state.tmval);
	if (t == (time_t)-1)
	  throw Exception(Error::HQL_PARSE_ERROR, std::string("DELETE invalid timestamp."));
	state.delete_time = (uint64_t)t * 1000000000LL + state.nanoseconds;
	memset(&state.tmval, 0, sizeof(state.tmval));
	state.nanoseconds = 0;
      }
      hql_interpreter_state &state;
    };



    struct hql_interpreter : public grammar<hql_interpreter> {
      hql_interpreter(hql_interpreter_state &state_) : state(state_) {}

      template <typename ScannerT>
      struct definition {

	definition(hql_interpreter const &self)  {
#ifdef BOOST_SPIRIT_DEBUG
	  debug(); // define the debug names
#endif

	  keywords =
	    "access", "ACCESS", "Access", "GROUP", "group", "Group",
	    "from", "FROM", "From", "start_time", "START_TIME", "Start_Time", "Start_time",
	    "end_time", "END_TIME", "End_Time", "End_time";

	  /**
	   * OPERATORS
	   */
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
	  chlit<>     LPAREN('(');
	  chlit<>     RPAREN(')');
	  chlit<>     LBRACK('[');
	  chlit<>     RBRACK(']');
	  chlit<>     POINTER('^');
	  chlit<>     DOT('.');
	  strlit<>    DOTDOT("..");
	  strlit<>    AND("&&");

	  /**
	   * TOKENS
	   */
	  typedef inhibit_case<strlit<> > token_t;

	  token_t CREATE       = as_lower_d["create"];
	  token_t DROP         = as_lower_d["drop"];
	  token_t HELP         = as_lower_d["help"];
	  token_t TABLE        = as_lower_d["table"];
	  token_t TABLES       = as_lower_d["tables"];
	  token_t TTL          = as_lower_d["ttl"];
	  token_t MONTHS       = as_lower_d["months"];
	  token_t MONTH        = as_lower_d["month"];
	  token_t WEEKS        = as_lower_d["weeks"];
	  token_t WEEK         = as_lower_d["week"];
	  token_t DAYS         = as_lower_d["days"];
	  token_t DAY          = as_lower_d["day"];
	  token_t HOURS        = as_lower_d["hours"];
	  token_t HOUR         = as_lower_d["hour"];
	  token_t MINUTES      = as_lower_d["minutes"];
	  token_t MINUTE       = as_lower_d["minute"];
	  token_t SECONDS      = as_lower_d["seconds"];
	  token_t SECOND       = as_lower_d["second"];
	  token_t IN_MEMORY    = as_lower_d["in_memory"];
	  token_t BLOCKSIZE    = as_lower_d["blocksize"];
	  token_t ACCESS       = as_lower_d["access"];
	  token_t GROUP        = as_lower_d["group"];
	  token_t DESCRIBE     = as_lower_d["describe"];
	  token_t SHOW         = as_lower_d["show"];
	  token_t ESC_HELP     = as_lower_d["\\h"];
	  token_t SELECT       = as_lower_d["select"];
	  token_t START_TIME   = as_lower_d["start_time"];
	  token_t END_TIME     = as_lower_d["end_time"];
	  token_t FROM         = as_lower_d["from"];
	  token_t WHERE        = as_lower_d["where"];
	  token_t ROW          = as_lower_d["row"];
	  token_t ROW_KEY_COLUMN = as_lower_d["row_key_column"];
	  token_t TIMESTAMP_COLUMN = as_lower_d["timestamp_column"];
	  token_t START_ROW    = as_lower_d["start_row"];
	  token_t END_ROW      = as_lower_d["end_row"];
	  token_t INCLUSIVE    = as_lower_d["inclusive"];
	  token_t EXCLUSIVE    = as_lower_d["exclusive"];
	  token_t MAX_VERSIONS = as_lower_d["max_versions"];
	  token_t LIMIT        = as_lower_d["limit"];
	  token_t INTO         = as_lower_d["into"];
	  token_t FILE         = as_lower_d["file"];
	  token_t LOAD         = as_lower_d["load"];
	  token_t DATA         = as_lower_d["data"];
	  token_t INFILE       = as_lower_d["infile"];
	  token_t TIMESTAMP    = as_lower_d["timestamp"];
	  token_t INSERT       = as_lower_d["insert"];
	  token_t DELETE       = as_lower_d["delete"];
	  token_t VALUES       = as_lower_d["values"];
	  token_t COMPRESSOR   = as_lower_d["compressor"];
	  token_t STARTS       = as_lower_d["starts"];
	  token_t WITH         = as_lower_d["with"];
	  token_t IF           = as_lower_d["if"];
	  token_t EXISTS       = as_lower_d["exists"];
	  token_t DISPLAY_TIMESTAMPS = as_lower_d["display_timestamps"];
	  token_t RETURN_DELETES = as_lower_d["return_deletes"];
	  token_t KEYS_ONLY    = as_lower_d["keys_only"];

	  /**
	   * Start grammar definition
	   */
	  identifier
	    = lexeme_d[
		(alpha_p >> *(alnum_p | '_'))
		- (keywords)
	    ];

	  string_literal 
	    = single_string_literal
	    | double_string_literal
	    ;

	  single_string_literal
	    = lexeme_d[ chlit<>('\'') >>
			+( anychar_p-chlit<>('\'') ) >>
			chlit<>('\'') ];

	  double_string_literal
	    = lexeme_d[ chlit<>('"') >>
			+( anychar_p-chlit<>('"') ) >>
			chlit<>('"') ];

	  user_identifier
	    = identifier
	    | string_literal
	    ;

	  statement
	    = select_statement[set_command(self.state, COMMAND_SELECT)]
	    | create_table_statement[set_command(self.state, COMMAND_CREATE_TABLE)]
	    | describe_table_statement[set_command(self.state, COMMAND_DESCRIBE_TABLE)]
	    | load_data_statement[set_command(self.state, COMMAND_LOAD_DATA)]
	    | show_statement[set_command(self.state, COMMAND_SHOW_CREATE_TABLE)]
	    | help_statement[set_help(self.state)]
	    | insert_statement[set_command(self.state, COMMAND_INSERT)]
	    | delete_statement[set_command(self.state, COMMAND_DELETE)]
	    | show_tables_statement[set_command(self.state, COMMAND_SHOW_TABLES)]
	    | drop_table_statement[set_command(self.state, COMMAND_DROP_TABLE)]
	    ;

	  drop_table_statement
	    = DROP >> TABLE >> !( IF >> EXISTS[set_if_exists(self.state)] ) >> user_identifier[set_table_name(self.state)] 
	    ;

	  show_tables_statement
	    = SHOW >> TABLES
	    ;

	  delete_statement
	    = DELETE >> delete_column_clause 
		     >> FROM >> user_identifier[set_table_name(self.state)] 
		     >> WHERE >> ROW >> EQUAL >> string_literal[delete_set_row(self.state)]
	             >> !( TIMESTAMP >> date_expression[set_delete_timestamp(self.state)] )
	    ;

	  delete_column_clause
	    = ( STAR[delete_column(self.state)] | (column_name[delete_column(self.state)] >> *( COMMA >> column_name[delete_column(self.state)] )) )
	    ;

	  insert_statement
	    = INSERT >> INTO >> identifier[set_table_name(self.state)] >> VALUES >> insert_value_list
	    ;

	  insert_value_list
	    = insert_value[add_insert_value(self.state)] >> *( COMMA >> insert_value[add_insert_value(self.state)] )
	    ;

	  insert_value
	    = LPAREN >> *( date_expression[set_insert_timestamp(self.state)] >> COMMA) 
		     >> string_literal[set_insert_rowkey(self.state)] >> COMMA
		     >> string_literal[set_insert_columnkey(self.state)] >> COMMA
		     >> string_literal[set_insert_value (self.state)] >> RPAREN
	    ;

	  show_statement
	    = ( SHOW >> CREATE >> TABLE >> identifier[set_table_name(self.state)] )
	    ;

	  help_statement
	    = ( HELP | ESC_HELP | QUESTIONMARK ) >> *anychar_p
	    ;

	  describe_table_statement
	    = DESCRIBE >> TABLE >> user_identifier[set_table_name(self.state)]
	    ;

	  create_table_statement
	    =  CREATE >> TABLE
		      >> *( table_option )
		      >> user_identifier[set_table_name(self.state)]
		      >> !( create_definitions )
	    ;

	  table_option
	    = COMPRESSOR >> EQUAL >> string_literal[set_table_compressor(self.state)]
	    ;

	  create_definitions 
	    = LPAREN >> create_definition 
		     >> *( COMMA >> create_definition ) 
		     >> RPAREN
	    ;

	  create_definition
	    = column_definition
	      | access_group_definition
	    ;

	  column_name 
	    = ( identifier | string_literal )
	    ;

	  column_definition
	    = column_name[create_column_family(self.state)] >> *( column_option )
	    ;

	  column_option
	    = max_versions_option
	    | ttl_option
	    ;

	  max_versions_option
	    = MAX_VERSIONS >> EQUAL >> lexeme_d[ (+digit_p)[set_column_family_max_versions(self.state)] ]
	    ;

	  ttl_option
	    = TTL >> EQUAL >> duration[set_ttl(self.state)]
	    ;

	  duration
	    = ureal_p >> 
	    (  MONTHS | MONTH | WEEKS | WEEK | DAYS | DAY | HOURS | HOUR | MINUTES | MINUTE | SECONDS | SECOND )
	    ;

	  access_group_definition
	    = ACCESS >> GROUP >> user_identifier[create_access_group(self.state)] 
		     >> *( access_group_option ) 
		     >> !( LPAREN >> column_name[add_column_family(self.state)]
		     >> *( COMMA >> column_name[add_column_family(self.state)] ) >> RPAREN )
	    ;

	  access_group_option
	    = in_memory_option[set_access_group_in_memory(self.state)]
	    | blocksize_option 
	    | COMPRESSOR >> EQUAL >> string_literal[set_access_group_compressor(self.state)]
	    ;

	  in_memory_option
	    = IN_MEMORY
	    ;

	  blocksize_option
	    = BLOCKSIZE >> EQUAL >> uint_p[set_access_group_blocksize(self.state)]
	    ;

	  select_statement
	    = SELECT 
	      >> ( '*' | ( user_identifier[scan_add_column_family(self.state)] >> *( COMMA >> user_identifier[scan_add_column_family(self.state)] ) ) ) 
	      >> FROM >> user_identifier[set_table_name(self.state)]
	      >> !where_clause
	      >> *( option_spec )
	    ;

	  where_clause
	    = WHERE >> where_predicate >> *( AND >> where_predicate )
	    ;

	  where_predicate
	    =  ROW >> EQUAL >> string_literal[scan_set_row(self.state)]
	    | ROW >> DOUBLEEQUAL >> string_literal[scan_set_row(self.state)]
	    | ROW >> GT >> string_literal[scan_set_start_row(self.state, false)]
	    | ROW >> GE >> string_literal[scan_set_start_row(self.state, true)]
	    | ROW >> LT >> string_literal[scan_set_end_row(self.state, false)]
	    | ROW >> LE >> string_literal[scan_set_end_row(self.state, true)]
	    | ROW >> STARTS >> WITH >> string_literal[scan_set_row_prefix(self.state)]
	    | TIMESTAMP >> GT >> date_expression[scan_set_start_time(self.state, false)]
	    | TIMESTAMP >> GE >> date_expression[scan_set_start_time(self.state, true)]
	    | TIMESTAMP >> LT >> date_expression[scan_set_end_time(self.state, false)]
	    | TIMESTAMP >> LE >> date_expression[scan_set_end_time(self.state, true)]
	    ;

	  option_spec
	    = START_TIME >> EQUAL >> date_expression[scan_set_start_time(self.state, true)]
	    | END_TIME >> EQUAL >> date_expression[scan_set_end_time(self.state, false)]
	    | MAX_VERSIONS >> EQUAL >> uint_p[scan_set_max_versions(self.state)]
	    | LIMIT >> EQUAL >> uint_p[scan_set_limit(self.state)]
	    | INTO >> FILE >> string_literal[scan_set_outfile(self.state)]
	    | DISPLAY_TIMESTAMPS[scan_set_display_timestamps(self.state)]
	    | RETURN_DELETES[scan_set_return_deletes(self.state)]
	    | KEYS_ONLY[scan_set_keys_only(self.state)]
	    ;

	  date_expression
	    = SINGLEQUOTE >> ( datetime | date | time | year ) >> SINGLEQUOTE 
	    | DOUBLEQUOTE >> ( datetime | date | time | year ) >> DOUBLEQUOTE 
	    ;

	  datetime
	    = date >> time
	    ;

	  uint_parser<unsigned int, 10, 2, 2> uint2_p;
	  uint_parser<unsigned int, 10, 4, 4> uint4_p;

	  date
	    = lexeme_d[
		       limit_d(0u, 9999u)[uint4_p[scan_set_year(self.state)]] >> '-'    //  Year
		       >>  limit_d(1u, 12u)[uint2_p[scan_set_month(self.state)]] >> '-' //  Month 01..12
		       >>  limit_d(1u, 31u)[uint2_p[scan_set_day(self.state)]]          //  Day 01..31
	    ];	  
	    

	  time 
	    = lexeme_d[
		       limit_d(0u, 23u)[uint2_p[scan_set_hours(self.state)]] >> ':'       //  Hours 00..23
		       >>  limit_d(0u, 59u)[uint2_p[scan_set_minutes(self.state)]] >> ':' //  Minutes 00..59
		       >>  limit_d(0u, 59u)[uint2_p[scan_set_seconds(self.state)]] >>     //  Seconds 00..59
		       !( DOT >> uint_p[scan_set_nanoseconds(self.state)] )
	    ];

	  year
	    = lexeme_d[ limit_d(0u, 9999u)[uint4_p[scan_set_year(self.state)]] ]
	    ;

	  load_data_statement
	    = LOAD >> DATA >> INFILE 
	    >> !( load_data_option >> *( load_data_option ) )
	    >> string_literal[set_str(self.state)] 
	    >> INTO >> 
	    ( TABLE >> user_identifier[set_table_name(self.state)]
	      | FILE >> user_identifier[set_output_file(self.state)] )
	    ;

	  load_data_option
	    = ROW_KEY_COLUMN >> EQUAL >> ( string_literal | identifier )[set_row_key_column(self.state)]
	    | TIMESTAMP_COLUMN >> EQUAL >> ( string_literal | identifier )[set_timestamp_column(self.state)]
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
	}
#endif

	rule<ScannerT> const&
	start() const { return statement; }
	symbols<> keywords;
	rule<ScannerT>
	column_definition, column_name, column_option, create_definition, create_definitions,
	create_table_statement, duration, identifier, user_identifier, max_versions_option,
        statement, single_string_literal, double_string_literal, string_literal,
        ttl_option, access_group_definition,
        access_group_option, in_memory_option, blocksize_option, help_statement,
        describe_table_statement, show_statement, select_statement, where_clause,
        where_predicate, option_spec, date_expression, datetime, date, time,
	year, load_data_statement, load_data_option, insert_statement, insert_value_list,
	insert_value, delete_statement, delete_column_clause, table_option,
        show_tables_statement, drop_table_statement;
      };

      hql_interpreter_state &state;
    };
  }
}

#endif // HYPERTABLE_HQLPARSER_H
