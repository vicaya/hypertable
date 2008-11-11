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

#include <cstdio>
#include <cstring>

extern "C" {
#include <time.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"
#include "Common/ScopeGuard.h"

#include "Client.h"
#include "HqlInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"
#include "Key.h"
#include "LoadDataSource.h"

using namespace std;
using namespace Hypertable;
using namespace Hql;

namespace {

void checked_fclose(FILE *fp, bool yes = true) {
  if (yes && fp)
    fclose(fp);
}

void cmd_help(ParserState &state, HqlInterpreter::Callback &cb) {
  const char **text = HqlHelpText::get(state.str);

  if (text) {
    for (; *text; ++text)
      cb.on_return(*text);
  }
  else
    cb.on_return("\nno help for '" + state.str);
}

void
cmd_show_create_table(Client *client, ParserState &state,
                      HqlInterpreter::Callback &cb) {
  String out_str;
  String schema_str = client->get_schema(state.table_name);
  Schema *schema = Schema::new_instance(schema_str.c_str(),
                                        schema_str.length(), true);
  if (!schema->is_valid())
    HT_THROW(Error::BAD_SCHEMA, schema->get_error_string());

  schema->render_hql_create_table(state.table_name, out_str);
  cb.on_return(out_str);
  cb.on_finish();
}

void
cmd_create_table(Client *client, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  String schema_str;
  Schema *schema;

  if (!state.clone_table_name.empty()) {
    schema_str = client->get_schema(state.clone_table_name);
    schema = Schema::new_instance(schema_str.c_str(), schema_str.size(), true);
    schema_str.clear();
    schema->render(schema_str);
    client->create_table(state.table_name, schema_str.c_str());
  }
  else {
    schema = new Schema();
    schema->set_compressor(state.table_compressor);

    foreach(Schema::AccessGroup *ag, state.ag_list)
      schema->add_access_group(ag);

    if (state.ag_map.find("default") == state.ag_map.end()) {
      Schema::AccessGroup *ag = new Schema::AccessGroup();
      ag->name = "default";
      schema->add_access_group(ag);
    }
    foreach(Schema::ColumnFamily *cf, state.cf_list) {
      if (cf->ag == "")
        cf->ag = "default";
      schema->add_column_family(cf);
    }
    const char *error_str = schema->get_error_string();

    if (error_str)
      HT_THROW(Error::HQL_PARSE_ERROR, error_str);

    schema->render(schema_str);
    client->create_table(state.table_name, schema_str.c_str());
  }
  cb.on_finish();
}

void
cmd_describe_table(Client *client, ParserState &state,
                   HqlInterpreter::Callback &cb) {
  String schema_str = client->get_schema(state.table_name);
  cb.on_return(schema_str);
  cb.on_finish();
}

void
cmd_select(Client *client, ParserState &state, HqlInterpreter::Callback &cb) {
  TablePtr table = client->open_table(state.table_name);
  TableScannerPtr scanner = table->create_scanner(state.scan.builder.get());
  FILE *outfp = cb.output;

  // whether it's select into file
  if (!state.scan.outfile.empty()) {
    FileUtils::expand_tilde(state.scan.outfile);
    if ((outfp = fopen(state.scan.outfile.c_str(), "w")) == 0)
      HT_THROWF(Error::EXTERNAL, "Unable to open file '%s' for writing - %s",
                state.scan.outfile.c_str(), strerror(errno));
    if (state.scan.display_timestamps) {
      if (state.scan.keys_only)
        fprintf(outfp, "#timestamp\trowkey\n");
      else
        fprintf(outfp, "#timestamp\trowkey\tcolumnkey\tvalue\n");
    }
    else {
      if (state.scan.keys_only)
        fprintf(outfp, "#rowkey\n");
      else
        fprintf(outfp, "#rowkey\tcolumnkey\tvalue\n");
    }
  }
  else if (!outfp) {
    cb.on_scan(*scanner.get());
    return;
  }

  HT_ON_SCOPE_EXIT(&checked_fclose, outfp, outfp != cb.output);

  Cell cell;
  uint32_t nsec;
  time_t unix_time;
  struct tm tms;
  uint64_t total_keys_size = 0;
  uint64_t total_values_size = 0;
  uint64_t total_cells = 0;

  while (scanner->next(cell)) {
    if (cb.normal_mode) {
      // do some stats
      ++total_cells;
      total_keys_size += strlen(cell.row_key);

      if (cell.column_family && cell.column_qualifier)
        total_keys_size += strlen(cell.column_qualifier) + 1;

      total_values_size += cell.value_len;
    }
    if (state.scan.display_timestamps) {
      if (cb.format_ts_in_usecs) {
        fprintf(outfp, "%llu\t", (Llu)cell.timestamp);
      }
      else {
        nsec = cell.timestamp % 1000000000LL;
        unix_time = cell.timestamp / 1000000000LL;
        gmtime_r(&unix_time, &tms);
        fprintf(outfp, "%d-%02d-%02d %02d:%02d:%02d.%09d\t", tms.tm_year+1900,
                tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min,
                tms.tm_sec, nsec);
      }
    }
    if (!state.scan.keys_only) {
      if (cell.column_family) {
        fprintf(outfp, "%s\t%s", cell.row_key, cell.column_family);
        if (*cell.column_qualifier)
          fprintf(outfp, ":%s", cell.column_qualifier);
      }
      else
        fprintf(outfp, "%s", cell.row_key);

      if (cell.flag != FLAG_INSERT) {
        fputc('\t', outfp);
        fwrite(cell.value, cell.value_len, 1, outfp);
        fputs("\tDELETE\n", outfp);
      }
      else {
        fputc('\t', outfp);
        fwrite(cell.value, cell.value_len, 1, outfp);
        fputc('\n', outfp);
      }
    }
    else
      fprintf(outfp, "%s\n", cell.row_key);
  }

  cb.on_finish(0, total_cells, total_keys_size, total_values_size);
}

void
cmd_load_data(Client *client, ParserState &state,
              HqlInterpreter::Callback &cb) {
  TablePtr table;
  TableMutatorPtr mutator;
  bool into_table = true;
  bool display_timestamps = false;
  FILE *outfp = cb.output;

  if (state.table_name.empty()) {
    if (state.output_file.empty())
      HT_THROW(Error::HQL_PARSE_ERROR,
               "LOAD DATA INFILE ... INTO FILE - bad filename");
    outfp = fopen(state.output_file.c_str(), "w");
    into_table = false;
  }
  else {
    table = client->open_table(state.table_name);
    mutator = table->create_mutator();
  }

  if (!FileUtils::exists(state.input_file.c_str()))
    HT_THROW(Error::FILE_NOT_FOUND, state.input_file);

  uint64_t file_size = FileUtils::size(state.input_file.c_str());
  cb.on_update(file_size);

  LoadDataSource lds(state.input_file, state.header_file, state.key_columns,
      state.timestamp_column, state.row_uniquify_chars, state.dupkeycols);

  if (!into_table) {
    display_timestamps = lds.has_timestamps();
    if (display_timestamps)
      fprintf(outfp, "timestamp\trowkey\tcolumnkey\tvalue\n");
    else
      fprintf(outfp, "rowkey\tcolumnkey\tvalue\n");
  }

  KeySpec key;
  uint8_t *value;
  uint32_t value_len;
  uint32_t consumed;
  uint64_t total_values_size = 0;
  uint64_t total_rowkey_size = 0;
  uint64_t insert_count = 0;

  while (lds.next(0, &key, &value, &value_len, &consumed)) {
    if (value_len > 0) {
      insert_count++;
      total_values_size += value_len;
      total_rowkey_size += key.row_len;

      if (into_table) {
        try {
          mutator->set(key, value, value_len);
        }
        catch (Exception &e) {
          do {
            mutator->show_failed(e);
          } while (!mutator->retry());
        }
      }
      else {
        if (display_timestamps)
          fprintf(outfp, "%llu\t%s\t%s\t%s\n", (Llu)key.timestamp,
                  (char *)key.row, key.column_family, (char *)value);
        else
          fprintf(outfp, "%s\t%s\t%s\n", (char *)key.row,
                  key.column_family, (char *)value);
      }
    }
    if (cb.normal_mode)
      cb.on_progress(consumed);
  }

  cb.on_finish(mutator.get(), insert_count, total_rowkey_size,
               total_values_size, file_size);
}

void
cmd_insert(Client *client, ParserState &state, HqlInterpreter::Callback &cb) {
  TablePtr table = client->open_table(state.table_name);
  TableMutatorPtr mutator = table->create_mutator();
  KeySpec key;
  char *column_qualifier;
  uint64_t total_cells = 0;
  uint64_t total_keys_size = 0;
  uint64_t total_values_size = 0;

  foreach(const InsertRecord &rec, state.inserts) {
    key.row = rec.row_key.c_str();
    key.row_len = rec.row_key.length();
    key.column_family = rec.column_key.c_str();
    if ((column_qualifier = strchr(rec.column_key.c_str(), ':')) != 0) {
      *column_qualifier++ = 0;
      key.column_qualifier = column_qualifier;
      key.column_qualifier_len = strlen(column_qualifier);
    }
    else {
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
    }
    key.timestamp = rec.timestamp;

    try {
      mutator->set(key, rec.value.c_str(), rec.value.length());
    }
    catch (Exception &e) {
      do {
        mutator->show_failed(e);
      } while (!mutator->retry());
    }
    if (cb.normal_mode) {
      ++total_cells;
      total_keys_size += key.column_qualifier_len + 1;
      total_values_size += rec.value.length();
    }
  }

  cb.on_finish(mutator.get(), total_cells, total_keys_size, total_values_size);
}

void
cmd_delete(Client *client, ParserState &state, HqlInterpreter::Callback &cb) {
  TablePtr table = client->open_table(state.table_name);
  TableMutatorPtr mutator = table->create_mutator();

  KeySpec key;
  char *column_qualifier;
  uint64_t total_cells = 0;

  key.row = state.delete_row.c_str();
  key.row_len = state.delete_row.length();

  if (state.delete_time != 0)
    key.timestamp = ++state.delete_time;
  else
    key.timestamp = AUTO_ASSIGN;

  if (state.delete_all_columns) {
    try {
      mutator->set_delete(key);
    }
    catch (Exception &e) {
      mutator->show_failed(e);
      return;
    }
  }
  else {
    foreach(const String &col, state.delete_columns) {
      ++total_cells;

      key.column_family = col.c_str();
      if ((column_qualifier = strchr(col.c_str(), ':')) != 0) {
        *column_qualifier++ = 0;
        key.column_qualifier = column_qualifier;
        key.column_qualifier_len = strlen(column_qualifier);
      }
      else {
        key.column_qualifier = 0;
        key.column_qualifier_len = 0;
      }
      try {
        mutator->set_delete(key);
      }
      catch (Exception &e) {
        mutator->show_failed(e);
        return;
      }
    }
  }

  cb.on_finish(mutator.get(), total_cells);
}

void
cmd_show_tables(Client *client, ParserState &state,
                HqlInterpreter::Callback &cb) {
  std::vector<String> tables;
  client->get_tables(tables);
  foreach (const String &t, tables) {
    cb.on_return(t);
  }
  cb.on_finish();
}

void
cmd_drop_table(Client *client, ParserState &state,
               HqlInterpreter::Callback &cb) {
  client->drop_table(state.table_name, state.if_exists);
  cb.on_finish();
}

void cmd_shutdown(Client *client, HqlInterpreter::Callback &cb) {
  client->shutdown();
  cb.on_finish();
}

} // local namespace


HqlInterpreter::HqlInterpreter(Client *client)
    : m_client(client) {
}


void HqlInterpreter::execute(const String &line, Callback &cb) {
  ParserState state;
  Hql::Parser parser(state);
  parse_info<> info = parse(line.c_str(), parser, space_p);

  if (info.full) {
    cb.on_parsed(state);

    switch (state.command) {
    case COMMAND_SHOW_CREATE_TABLE:
      cmd_show_create_table(m_client, state, cb);               break;
    case COMMAND_HELP:
      cmd_help(state, cb);                                      break;
    case COMMAND_CREATE_TABLE:
      cmd_create_table(m_client, state, cb);                    break;
    case COMMAND_DESCRIBE_TABLE:
      cmd_describe_table(m_client, state, cb);                  break;
    case COMMAND_SELECT:
      cmd_select(m_client, state, cb);                          break;
    case COMMAND_LOAD_DATA:
      cmd_load_data(m_client, state, cb);                       break;
    case COMMAND_INSERT:
      cmd_insert(m_client, state, cb);                          break;
    case COMMAND_DELETE:
      cmd_delete(m_client, state, cb);                          break;
    case COMMAND_SHOW_TABLES:
      cmd_show_tables(m_client, state, cb);                     break;
    case COMMAND_DROP_TABLE:
      cmd_drop_table(m_client, state, cb);                      break;
    case COMMAND_SHUTDOWN:
      cmd_shutdown(m_client, cb);                               break;
    default:
      HT_THROW(Error::HQL_PARSE_ERROR, String("unsupported command: ") + line);
    }
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop);
}
