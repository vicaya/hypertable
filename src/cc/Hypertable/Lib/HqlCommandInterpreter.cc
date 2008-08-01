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

#include "Common/Compat.h"
#include "Schema.h"


#include <cassert>
#include <cstdio>
#include <cstring>

#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"
#include "Key.h"
#include "LoadDataSource.h"

using namespace std;
using namespace Hypertable;
using namespace HqlParser;

namespace {

  bool display_mutation_errors(int error, const char *what, TableMutator *mutator) {
    std::vector<std::pair<Cell, int> > failed_mutations;

    mutator->get_failed(failed_mutations);

    if (!failed_mutations.empty()) {
      for (size_t i=0; i<failed_mutations.size(); i++) {
        cout << "Failed: (" << failed_mutations[i].first.row_key << ","
             << failed_mutations[i].first.column_family;

        if (failed_mutations[i].first.column_qualifier &&
            *(failed_mutations[i].first.column_qualifier))
          cout << ":" << failed_mutations[i].first.column_qualifier;

        cout << "," << failed_mutations[i].first.timestamp << ") - "
             << Error::get_text(failed_mutations[i].second) << endl;
      }
    }
    else {
      cerr << "Error: " << Error::get_text(error) << " - " << what << endl;
      return false;
    }
    return true;
  }

}


/**
 *
 */
HqlCommandInterpreter::HqlCommandInterpreter(Client *client)
    : m_client(client) {
}


/**
 *
 */
void HqlCommandInterpreter::execute_line(const String &line) {
  String schema_str;
  String out_str;
  hql_interpreter_state state;
  hql_interpreter interp(state);
  parse_info<> info;
  Schema *schema;

  info = parse(line.c_str(), interp, space_p);

  if (info.full) {

    if (state.command == COMMAND_SHOW_CREATE_TABLE) {
      schema_str = m_client->get_schema(state.table_name);
      schema = Schema::new_instance(schema_str.c_str(),
                                    schema_str.length(), true);
      if (!schema->is_valid())
        HT_THROW(Error::BAD_SCHEMA, schema->get_error_string());

      schema->render_hql_create_table(state.table_name, out_str);
      cout << out_str << flush;
    }
    else if (state.command == COMMAND_HELP) {
      const char **text = HqlHelpText::Get(state.str);

      if (text) {
        for (size_t i=0; text[i]; i++)
          cout << text[i] << endl;
      }
      else
        cout << endl << "no help for '" << state.str << "'" << endl << endl;
    }
    else if (state.command == COMMAND_CREATE_TABLE) {
      if (!state.clone_table_name.empty()) {
        schema_str = m_client->get_schema(state.clone_table_name);
        schema = Schema::new_instance(schema_str.c_str(), strlen(schema_str.c_str()), true);
        schema_str.clear();
        schema->render(schema_str);
        m_client->create_table(state.table_name, schema_str.c_str());
      } else {
        schema = new Schema();
        schema->set_compressor(state.table_compressor);

        foreach(const Schema::AccessGroupMap::value_type &v, state.ag_map)
          schema->add_access_group(v.second);

        if (state.ag_map.find("default") == state.ag_map.end()) {
          Schema::AccessGroup *ag = new Schema::AccessGroup();
          ag->name = "default";
          schema->add_access_group(ag);
        }
        foreach(const Schema::ColumnFamilyMap::value_type &v, state.cf_map) {
          if (v.second->ag == "")
            v.second->ag = "default";
          schema->add_column_family(v.second);
        }
        const char *error_str = schema->get_error_string();

        if (error_str)
          HT_THROW(Error::HQL_PARSE_ERROR, error_str);
        schema->render(schema_str);
        m_client->create_table(state.table_name, schema_str.c_str());
      }
    }
    else if (state.command == COMMAND_DESCRIBE_TABLE) {
      schema_str = m_client->get_schema(state.table_name);
      cout << schema_str << endl;
    }
    else if (state.command == COMMAND_SELECT) {
      TablePtr table_ptr;
      TableScannerPtr scanner_ptr;
      ScanSpec scan_spec;
      Cell cell;
      uint32_t nsec;
      time_t unix_time;
      struct tm tms;
      RowInterval  ri;
      CellInterval ci;

      scan_spec.row_limit = state.scan.limit;
      scan_spec.max_versions = state.scan.max_versions;
      for (size_t i=0; i<state.scan.columns.size(); i++)
        scan_spec.columns.push_back(state.scan.columns[i].c_str());

      if (state.scan.row_intervals.size() && state.scan.cell_intervals.size())
	HT_THROW(Error::HQL_PARSE_ERROR, "ROW predicates and CELL predicates can't be combined");

      for (size_t i=0; i<state.scan.row_intervals.size(); i++) {
	ri.start = state.scan.row_intervals[i].start.c_str();
	ri.start_inclusive = state.scan.row_intervals[i].start_inclusive;
	ri.end = state.scan.row_intervals[i].end.c_str();
	ri.end_inclusive = state.scan.row_intervals[i].end_inclusive;
	scan_spec.row_intervals.push_back(ri);
      }

      for (size_t i=0; i<state.scan.cell_intervals.size(); i++) {
	ci.start_row = state.scan.cell_intervals[i].start_row.c_str();
	ci.start_column = state.scan.cell_intervals[i].start_column.c_str();
	ci.start_inclusive = state.scan.cell_intervals[i].start_inclusive;
	ci.end_row = state.scan.cell_intervals[i].end_row.c_str();
	ci.end_column = state.scan.cell_intervals[i].end_column.c_str();
	ci.end_inclusive = state.scan.cell_intervals[i].end_inclusive;
	scan_spec.cell_intervals.push_back(ci);
      }

      scan_spec.time_interval.first  = state.scan.start_time;
      scan_spec.time_interval.second = state.scan.end_time;
      scan_spec.return_deletes = state.scan.return_deletes;

      table_ptr = m_client->open_table(state.table_name);

      scanner_ptr = table_ptr->create_scanner(scan_spec);

      while (scanner_ptr->next(cell)) {
        if (state.scan.display_timestamps) {
          if (m_timestamp_output_format == TIMESTAMP_FORMAT_USECS) {
            printf("%llu\t", (long long unsigned int)cell.timestamp);
          }
          else {
            nsec = cell.timestamp % 1000000000LL;
            unix_time = cell.timestamp / 1000000000LL;
            gmtime_r(&unix_time, &tms);
            printf("%d-%02d-%02d %02d:%02d:%02d.%09d\t", tms.tm_year+1900,
                   tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min,
                   tms.tm_sec, nsec);
          }
        }
        if (!state.scan.keys_only) {
          if (cell.column_family) {
            printf("%s\t%s", cell.row_key, cell.column_family);
            if (*cell.column_qualifier)
              printf(":%s", cell.column_qualifier);
          }
          else
            printf("%s", cell.row_key);
          if (cell.flag != FLAG_INSERT)
            printf("\t%s\tDELETE\n", String((const char *)cell.value,
                                            cell.value_len).c_str());
          else
            printf("\t%s\n", String((const char *)cell.value,
                                     cell.value_len).c_str());
        }
        else
          printf("%s\n", cell.row_key);
      }
    }
    else if (state.command == COMMAND_LOAD_DATA) {
      TablePtr table_ptr;
      TableMutatorPtr mutator_ptr;
      uint64_t timestamp;
      KeySpec key;
      uint8_t *value;
      uint32_t value_len;
      uint32_t consumed;
      uint64_t file_size;
      uint64_t total_values_size = 0;
      uint64_t total_rowkey_size = 0;
      string start_msg;
      uint64_t insert_count = 0;
      Stopwatch stopwatch;
      bool into_table = true;
      bool display_timestamps = false;
      FILE *outfp = 0;

      if (state.table_name == "") {
        if (state.output_file == "")
          HT_THROW(Error::HQL_PARSE_ERROR,
                   "LOAD DATA INFILE ... INTO FILE - bad filename");
        outfp = fopen(state.output_file.c_str(), "w");
        into_table = false;
      }
      else {
        table_ptr = m_client->open_table(state.table_name);
        mutator_ptr = table_ptr->create_mutator();
      }

      if (!FileUtils::exists(state.input_file.c_str()))
        HT_THROW(Error::FILE_NOT_FOUND, state.input_file);

      file_size = FileUtils::size(state.input_file.c_str());

      if (!m_silent && !m_test_mode) {
	printf("\nLoading ");
	const char *format = "%3lld,";
	if (file_size > 1000000000000LL) {
	  printf(format, file_size/1000000000000LL);
	  format = "%03lld,";
	}
	if (file_size > 1000000000LL) {
	  printf(format, (file_size%1000000000000LL) / 1000000000LL);
	  format = "%03lld,";
	}
	if (file_size > 1000000LL) {
	  printf(format, (file_size%1000000000LL) / 1000000LL);
	  format = "%03lld,";
	}
	if (file_size > 1000LL) {
	  printf(format, (file_size%1000000LL) / 1000LL);
	  printf("%03lld", file_size % 1000LL);
	}
	else
	  printf("%3lld", file_size % 1000LL);
	printf(" bytes of input data...\n");
	fflush(stdout);
      }

      boost::progress_display *show_progress = 0;

      if (!m_silent && !m_test_mode)
	show_progress = new boost::progress_display(file_size);

      auto_ptr<LoadDataSource> lds(new LoadDataSource(state.input_file,
          state.header_file, state.key_columns, state.timestamp_column,
          state.row_uniquify_chars, state.dupkeycols));

      if (!into_table) {
        display_timestamps = lds->has_timestamps();
        if (display_timestamps)
          fprintf(outfp, "timestamp\trowkey\tcolumnkey\tvalue\n");
        else
          fprintf(outfp, "rowkey\tcolumnkey\tvalue\n");
      }

      while (lds->next(0, &timestamp, &key, &value, &value_len, &consumed)) {
        if (value_len > 0) {
          insert_count++;
          total_values_size += value_len;
          total_rowkey_size += key.row_len;
          if (into_table) {
            try {
              mutator_ptr->set(timestamp, key, value, value_len);
            }
            catch (Hypertable::Exception &e) {
              do {
                if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
		  return;
              } while (!mutator_ptr->retry(30));
            }
          }
          else {
            if (display_timestamps)
              fprintf(outfp, "%llu\t%s\t%s\t%s\n", (Llu)timestamp,
                      (const char *)key.row, key.column_family,
                      (const char *)value);
            else
              fprintf(outfp, "%s\t%s\t%s\n", (const char *)key.row,
                      key.column_family, (const char *)value);
          }
        }
	if (!m_silent && !m_test_mode)
	  *show_progress += consumed;
      }

      if (into_table) {
        // Flush pending updates
        try {
          mutator_ptr->flush();
        }
        catch (Exception &e) {
          do {
            if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
	      return;
          } while (!mutator_ptr->retry(30));
        }
      }
      else
        fclose(outfp);

      if (!m_silent && !m_test_mode && show_progress->count() < file_size)
        *show_progress += file_size - show_progress->count();

      delete show_progress;

      stopwatch.stop();

      if (!m_silent && !m_test_mode) {
	printf("Load complete.\n");
	printf("\n");
	printf("  Elapsed time:  %.2f s\n", stopwatch.elapsed());
	printf("Avg value size:  %.2f bytes\n",
               (double)total_values_size / insert_count);
	printf("  Avg key size:  %.2f bytes\n",
               (double)total_rowkey_size / insert_count);
	printf("    Throughput:  %.2f bytes/s\n",
               (double)file_size / stopwatch.elapsed());
	printf(" Total inserts:  %llu\n", (Llu)insert_count);
	printf("    Throughput:  %.2f inserts/s\n",
               (double)insert_count / stopwatch.elapsed());
	if (mutator_ptr)
	  printf("       Resends:  %llu\n",
                 (Llu)mutator_ptr->get_resend_count());
	printf("\n");
      }
    }
    else if (state.command == COMMAND_INSERT) {
      TablePtr table_ptr;
      TableMutatorPtr mutator_ptr;
      KeySpec key;
      char *column_qualifier;
      String tmp_str;

      table_ptr = m_client->open_table(state.table_name);

      mutator_ptr = table_ptr->create_mutator();

      for (size_t i=0; i<state.inserts.size(); i++) {
        key.row = state.inserts[i].row_key.c_str();
        key.row_len = state.inserts[i].row_key.length();
        key.column_family = state.inserts[i].column_key.c_str();
        if ((column_qualifier = strchr(state.inserts[i].column_key.c_str(), ':')
            ) != 0) {
          *column_qualifier++ = 0;
          key.column_qualifier = column_qualifier;
          key.column_qualifier_len = strlen(column_qualifier);
        }
        else {
          key.column_qualifier = 0;
          key.column_qualifier_len = 0;
        }
        try {
          mutator_ptr->set(state.inserts[i].timestamp, key,
                           (uint8_t *)state.inserts[i].value.c_str(),
                           (uint32_t)state.inserts[i].value.length());
        }
        catch (Hypertable::Exception &e) {
          if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
	    return;
        }
      }

      try {
        mutator_ptr->flush();
      }
      catch (Exception &e) {
        if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
	  return;
      }
    }
    else if (state.command == COMMAND_DELETE) {
      TablePtr table_ptr;
      TableMutatorPtr mutator_ptr;
      KeySpec key;
      char *column_qualifier;
      String tmp_str;

      table_ptr = m_client->open_table(state.table_name);

      mutator_ptr = table_ptr->create_mutator();

      memset(&key, 0, sizeof(key));
      key.row = state.delete_row.c_str();
      key.row_len = state.delete_row.length();

      if (state.delete_time != 0)
        state.delete_time++;

      if (state.delete_all_columns) {
        try {
          mutator_ptr->set_delete(state.delete_time, key);
        }
        catch (Hypertable::Exception &e) {
          display_mutation_errors(e.code(), e.what(), mutator_ptr.get());
          return;
        }
      }
      else {
        for (size_t i=0; i<state.delete_columns.size(); i++) {
          key.column_family = state.delete_columns[i].c_str();
          if ((column_qualifier = strchr(state.delete_columns[i].c_str(), ':'))
              != 0) {
            *column_qualifier++ = 0;
            key.column_qualifier = column_qualifier;
            key.column_qualifier_len = strlen(column_qualifier);
          }
          else {
            key.column_qualifier = 0;
            key.column_qualifier_len = 0;
          }
          try {
            mutator_ptr->set_delete(state.delete_time, key);
          }
          catch (Hypertable::Exception &e) {
            if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
	      return;
          }
        }
      }
      try {
        mutator_ptr->flush();
      }
      catch (Exception &e) {
        if (!display_mutation_errors(e.code(), e.what(), mutator_ptr.get()))
	  return;
      }
    }
    else if (state.command == COMMAND_SHOW_TABLES) {
      std::vector<String> tables;
      m_client->get_tables(tables);
      if (tables.empty())
        cout << "Empty set" << endl;
      else {
        for (size_t i=0; i<tables.size(); i++)
          cout << tables[i] << endl;
      }
    }
    else if (state.command == COMMAND_DROP_TABLE) {
      m_client->drop_table(state.table_name, state.if_exists);
    }
    else if (state.command == COMMAND_SHUTDOWN) {
      m_client->shutdown();
    }
    else
      HT_THROW(Error::HQL_PARSE_ERROR, String("unsupported command: ") + line);
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop);

}
