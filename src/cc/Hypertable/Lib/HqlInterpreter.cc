/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Zvents, Inc.)
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
#include <sstream>
#include <iostream>

extern "C" {
#include <time.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/iostreams/device/file_descriptor.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/device/null.hpp>

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"
#include "Common/ScopeGuard.h"
#include "Common/String.h"

#include "Client.h"
#include "Namespace.h"
#include "HqlInterpreter.h"
#include "HqlHelpText.h"
#include "HqlParser.h"
#include "Key.h"
#include "LoadDataEscape.h"
#include "LoadDataFlags.h"
#include "LoadDataSource.h"
#include "LoadDataSourceFactory.h"
#include "ScanSpec.h"
#include "TableSplit.h"
#include "Types.h"

#include "DfsBroker/Lib/FileDevice.h"

using namespace std;
using namespace Hypertable;
using namespace Hql;

namespace {

void close_file(int fd) {
  if (fd >= 0)
    close(fd);
}

void cmd_help(ParserState &state, HqlInterpreter::Callback &cb) {
  const char **text = HqlHelpText::get(state.str);

  if (text) {
    for (; *text; ++text)
      cb.on_return(*text);
  }
  else
    cb.on_return("\no help for '" + state.str);
}

void
cmd_create_namespace(Client *client, NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {

  if (!ns || state.ns.find('/') == 0)
    client->create_namespace(state.ns);
  else
    client->create_namespace(state.ns, ns.get());
  cb.on_finish();
}

void
cmd_use_namespace(Client *client, NamespacePtr &ns, bool immutable_namespace,
                  ParserState &state, HqlInterpreter::Callback &cb) {

  if (ns && immutable_namespace)
    HT_THROW(Error::BAD_NAMESPACE, (String)"Attempting to modify immutable namespace " +
             ns->get_name() + " to " + state.ns);

  NamespacePtr tmp_ns;
  if (!ns || state.ns.find('/') == 0)
    tmp_ns = client->open_namespace(state.ns);
  else
    tmp_ns = client->open_namespace(state.ns, ns.get());
  if (tmp_ns)
    ns = tmp_ns;
  else
    HT_THROW(Error::NAMESPACE_DOES_NOT_EXIST, (String)"Couldn't open namespace " + state.ns);

  cb.on_finish();
}

void
cmd_drop_namespace(Client *client, NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {

  if (!ns || state.ns.find('/') == 0 )
    client->drop_namespace(state.ns, NULL, state.if_exists);
  else
    client->drop_namespace(state.ns, ns.get(), state.if_exists);
  cb.on_finish();
}

void
cmd_rename_table(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  ns->rename_table(state.table_name, state.new_table_name);
  cb.on_finish();
}

void
cmd_exists_table(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  String exists = (String)"true";

  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  if (!ns->exists_table(state.table_name))
    exists = (String)"false";
  cb.on_return(exists);
  cb.on_finish();
}

void
cmd_show_create_table(NamespacePtr &ns, ParserState &state,
                      HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String out_str;
  String schema_str = ns->get_schema_str(state.table_name, true);
  SchemaPtr schema = Schema::new_instance(schema_str.c_str(),
                                          schema_str.length());
  if (!schema->is_valid())
    HT_THROW(Error::BAD_SCHEMA, schema->get_error_string());

  schema->render_hql_create_table(state.table_name, out_str);
  cb.on_return(out_str);
  cb.on_finish();
}


void
cmd_create_table(NamespacePtr &ns, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str;
  SchemaPtr schema;
  bool need_default_ag = false;

  if (!state.clone_table_name.empty()) {
    schema_str = ns->get_schema_str(state.clone_table_name, true);
    schema = Schema::new_instance(schema_str.c_str(), schema_str.size());
    schema_str.clear();
    schema->render(schema_str);
    ns->create_table(state.table_name, schema_str.c_str());
  }
  else {
    schema = new Schema();
    schema->validate_compressor(state.table_compressor);
    schema->set_compressor(state.table_compressor);
    schema->set_group_commit_interval(state.group_commit_interval);

    foreach(Schema::AccessGroup *ag, state.ag_list) {
      schema->validate_compressor(ag->compressor);
      schema->validate_bloom_filter(ag->bloom_filter);
      if (state.table_in_memory)
        ag->in_memory = true;
      if (state.table_blocksize != 0 && ag->blocksize == 0)
        ag->blocksize = state.table_blocksize;
      if (state.table_replication != -1 && ag->replication == -1)
        ag->replication = (int16_t)state.table_replication;
      schema->add_access_group(ag);
    }

    if (state.ag_map.find("default") == state.ag_map.end())
      need_default_ag = true;

    foreach(Schema::ColumnFamily *cf, state.cf_list) {
      if (cf->ag == "") {
        cf->ag = "default";
        if (need_default_ag) {
          Schema::AccessGroup *ag = new Schema::AccessGroup();
          ag->name = "default";
          if (state.table_in_memory)
            ag->in_memory = true;
          if (state.table_blocksize != 0 && ag->blocksize == 0)
            ag->blocksize = state.table_blocksize;
          if (state.table_replication != -1 && ag->replication == -1)
            ag->replication = (int16_t)state.table_replication;
          schema->add_access_group(ag);
          need_default_ag = false;
        }
      }
      if (cf->ttl == 0 && state.ttl != 0)
        cf->ttl = state.ttl;
      if (cf->max_versions == 0 && state.max_versions != 0)
        cf->max_versions = state.max_versions;
      if (cf->max_versions != 0 && cf->counter)
        HT_THROWF(Error::HQL_PARSE_ERROR,
                  "Incompatible options (COUNTER & MAX_VERSIONS) specified for column '%s'",
                  cf->name.c_str());
      schema->add_column_family(cf);
    }

    if (!schema->is_valid())
      HT_THROW(Error::HQL_PARSE_ERROR, schema->get_error_string());

    schema->render(schema_str);
    ns->create_table(state.table_name, schema_str.c_str());
  }
  cb.on_finish();
}

void
cmd_alter_table(NamespacePtr &ns, ParserState &state,
                 HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str;
  Schema *schema = new Schema();
  bool need_default_ag = false;

  foreach(Schema::AccessGroup *ag, state.ag_list)
    schema->add_access_group(ag);

  if (state.ag_map.find("default") == state.ag_map.end())
    need_default_ag = true;

  foreach(Schema::ColumnFamily *cf, state.cf_list) {
    if (cf->ag == "") {
      cf->ag = "default";
      if (need_default_ag) {
        Schema::AccessGroup *ag = new Schema::AccessGroup();
        ag->name = "default";
        schema->add_access_group(ag);
        need_default_ag = false;
      }
    }
    schema->add_column_family(cf);
  }

  const char *error_str = schema->get_error_string();

  if (error_str)
    HT_THROW(Error::HQL_PARSE_ERROR, error_str);

  schema->render(schema_str);
  ns->alter_table(state.table_name, schema_str.c_str());

  /**
   * Refresh the cached table
   */
  ns->refresh_table(state.table_name);
  cb.on_finish();
}

void
cmd_describe_table(NamespacePtr &ns, ParserState &state,
                   HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  String schema_str = ns->get_schema_str(state.table_name, state.with_ids);
  cb.on_return(schema_str);
  cb.on_finish();
}

void
cmd_select(NamespacePtr &ns, ConnectionManagerPtr &conn_manager,
           DfsBroker::ClientPtr &dfs_client, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableScannerSyncPtr scanner;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  String dfs = "dfs://";
  String localfs = "file://";

  table = ns->open_table(state.table_name);
  scanner = table->create_scanner_sync(state.scan.builder.get(), 0, true);

  // whether it's select into file
  if (!state.scan.outfile.empty()) {
    FileUtils::expand_tilde(state.scan.outfile);

    if (boost::algorithm::ends_with(state.scan.outfile, ".gz"))
      fout.push(boost::iostreams::gzip_compressor());

    if (boost::algorithm::starts_with(state.scan.outfile, dfs)) {
      // init Dfs client if not done yet
      if (!dfs_client)
        dfs_client = new DfsBroker::Client(conn_manager, Config::properties);
      fout.push(DfsBroker::FileSink(dfs_client, state.scan.outfile.substr(dfs.size())));
    }
    else if (boost::algorithm::starts_with(state.scan.outfile, localfs))
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile.substr(localfs.size())));
    else
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile));

    if (state.scan.display_timestamps) {
      if (state.scan.keys_only)
        fout << "#timestamp\trow\n";
      else
        fout << "#timestamp\trow\tcolumn\tvalue\n";
    }
    else {
      if (state.scan.keys_only)
        fout << "#row\n";
      else
        fout << "#row\tcolumn\tvalue\n";
    }
  }
  else if (!outf) {
    cb.on_scan(*scanner.get());
    return;
  }
  else {
    out_fd = dup(fileno(outf));
    fout.push(boost::iostreams::file_descriptor_sink(out_fd));
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);
  Cell cell;
  ::uint32_t nsec;
  time_t unix_time;
  struct tm tms;
  LoadDataEscape row_escaper;
  LoadDataEscape escaper;
  const char *unescaped_buf, *row_unescaped_buf;
  size_t unescaped_len, row_unescaped_len;

  while (scanner->next(cell)) {
    if (cb.normal_mode) {
      // do some stats
      ++cb.total_cells;
      cb.total_keys_size += strlen(cell.row_key);

      if (cell.column_family && cell.column_qualifier)
        cb.total_keys_size += strlen(cell.column_qualifier) + 1;

      cb.total_values_size += cell.value_len;
    }
    if (state.scan.display_timestamps) {
      if (cb.format_ts_in_usecs) {
        fout << cell.timestamp << "\t";
      }
      else {
        nsec = cell.timestamp % 1000000000LL;
        unix_time = cell.timestamp / 1000000000LL;
        gmtime_r(&unix_time, &tms);
        fout << format("%d-%02d-%02d %02d:%02d:%02d.%09d\t", tms.tm_year+1900,
                       tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min, tms.tm_sec, nsec);
      }
    }
    if (state.escape)
      row_escaper.escape(cell.row_key, strlen(cell.row_key),
			 &row_unescaped_buf, &row_unescaped_len);
    else
      row_unescaped_buf = cell.row_key;
    if (!state.scan.keys_only) {
      if (cell.column_family && *cell.column_family) {
        fout << row_unescaped_buf << "\t" << cell.column_family;
        if (cell.column_qualifier && *cell.column_qualifier) {
          if (state.escape)
            escaper.escape(cell.column_qualifier, strlen(cell.column_qualifier),
                &unescaped_buf, &unescaped_len);
          else
            unescaped_buf = cell.column_qualifier;
          fout << ":" << unescaped_buf;
        }

      }
      else
        fout << row_unescaped_buf;

      if (state.escape)
        escaper.escape((const char *)cell.value, (size_t)cell.value_len,
                       &unescaped_buf, &unescaped_len);
      else {
        unescaped_buf = (const char *)cell.value;
        unescaped_len = (size_t)cell.value_len;
      }

      fout << "\t" ;
      switch(cell.flag) {
      case FLAG_INSERT:
        fout.write(unescaped_buf, unescaped_len);
        fout << "\n";
        break;
      case FLAG_DELETE_ROW:
        fout << "\tDELETE ROW\n";
        break;
       case FLAG_DELETE_COLUMN_FAMILY:
        fout.write(unescaped_buf, unescaped_len);
        fout << "\tDELETE COLUMN FAMILY\n";
        break;
      case FLAG_DELETE_CELL:
        fout.write(unescaped_buf, unescaped_len);
        fout << "\tDELETE CELL\n";
        break;
      default:
        fout << "\tBAD KEY FLAG\n";
      }
    }
    else
      fout << row_unescaped_buf << "\n";
  }

  fout.strict_sync();

  cb.on_finish(0);
}


void
cmd_dump_table(NamespacePtr &ns,
               ConnectionManagerPtr &conn_manager, DfsBroker::ClientPtr &dfs_client,
               ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  String dfs = "dfs://";
  String localfs = "file://";

  // verify parameters

  TableDumperPtr dumper = new TableDumper(ns, state.table_name, state.scan.builder.get());

  // whether it's select into file
  if (!state.scan.outfile.empty()) {
    FileUtils::expand_tilde(state.scan.outfile);

    if (boost::algorithm::ends_with(state.scan.outfile, ".gz"))
      fout.push(boost::iostreams::gzip_compressor());
    if (boost::algorithm::starts_with(state.scan.outfile, dfs)) {
      // init Dfs client if not done yet
      if (!dfs_client)
        dfs_client = new DfsBroker::Client(conn_manager, Config::properties);
      fout.push(DfsBroker::FileSink(dfs_client, state.scan.outfile.substr(dfs.size())));
    }
    else if (boost::algorithm::starts_with(state.scan.outfile, localfs))
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile.substr(localfs.size())));
    else
      fout.push(boost::iostreams::file_descriptor_sink(state.scan.outfile));

    fout << "#timestamp\trow\tcolumn\tvalue\n";
  }
  else if (!outf) {
    cb.on_dump(*dumper.get());
    return;
  }
  else {
    out_fd = dup(fileno(outf));
    fout.push(boost::iostreams::file_descriptor_sink(out_fd));
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);
  Cell cell;
  LoadDataEscape row_escaper;
  LoadDataEscape escaper;
  const char *unescaped_buf, *row_unescaped_buf;
  size_t unescaped_len, row_unescaped_len;

  while (dumper->next(cell)) {
    if (cb.normal_mode) {
      // do some stats
      ++cb.total_cells;
      cb.total_keys_size += strlen(cell.row_key);

      if (cell.column_family && cell.column_qualifier)
        cb.total_keys_size += strlen(cell.column_qualifier) + 1;

      cb.total_values_size += cell.value_len;
    }

    fout << cell.timestamp << "\t";

    if (state.escape)
      row_escaper.escape(cell.row_key, strlen(cell.row_key),
			 &row_unescaped_buf, &row_unescaped_len);
    else
      row_unescaped_buf = cell.row_key;

    if (cell.column_family) {
      fout << row_unescaped_buf << "\t" << cell.column_family;
      if (cell.column_qualifier && *cell.column_qualifier) {
	if (state.escape)
	  escaper.escape(cell.column_qualifier, strlen(cell.column_qualifier),
			 &unescaped_buf, &unescaped_len);
	else
	  unescaped_buf = cell.column_qualifier;
	fout << ":" << unescaped_buf;
      }
    }
    else
      fout << row_unescaped_buf;

    if (state.escape)
      escaper.escape((const char *)cell.value, (size_t)cell.value_len,
		     &unescaped_buf, &unescaped_len);
    else {
      unescaped_buf = (const char *)cell.value;
      unescaped_len = (size_t)cell.value_len;
    }

    HT_ASSERT(cell.flag == FLAG_INSERT);

    fout << "\t" ;
    fout.write(unescaped_buf, unescaped_len);
    fout << "\n";
  }

  fout.strict_sync();

  cb.on_finish(0);
}

void
cmd_load_data(NamespacePtr &ns, ::uint32_t mutator_flags,
              ConnectionManagerPtr &conn_manager, DfsBroker::ClientPtr &dfs_client,
              ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  bool into_table = true;
  bool display_timestamps = false;
  boost::iostreams::filtering_ostream fout;
  FILE *outf = cb.output;
  int out_fd = -1;
  bool largefile_mode = false;
  int64_t last_total = 0, new_total;

  if (LoadDataFlags::ignore_unknown_cfs(state.load_flags))
    mutator_flags |= TableMutator::FLAG_IGNORE_UNKNOWN_CFS;

  if (state.table_name.empty()) {
    if (state.output_file.empty())
      HT_THROW(Error::HQL_PARSE_ERROR,
          "LOAD DATA INFILE ... INTO FILE - bad filename");
    fout.push(boost::iostreams::file_descriptor_sink(state.output_file));
    into_table = false;
  }
  else {
    if (outf) {
      out_fd = dup(fileno(outf));
      fout.push(boost::iostreams::file_descriptor_sink(out_fd));
    }
    else
      fout.push(boost::iostreams::null_sink());
    table = ns->open_table(state.table_name);
    mutator = table->create_mutator(0, mutator_flags);
  }

  HT_ON_SCOPE_EXIT(&close_file, out_fd);


  LoadDataSourcePtr lds;

  // init Dfs client if not done yet
  if(state.input_file_src == DFS_FILE && !dfs_client)
    dfs_client = new DfsBroker::Client(conn_manager, Config::properties);

  lds = LoadDataSourceFactory::create(dfs_client, state.input_file, state.input_file_src,
      state.header_file, state.header_file_src,
      state.key_columns, state.timestamp_column,
      state.row_uniquify_chars, state.load_flags);

  cb.file_size = lds->get_source_size();
  if (cb.file_size > std::numeric_limits<unsigned long>::max()) {
    largefile_mode = true;
    unsigned long adjusted_size = (unsigned long)(cb.file_size / 1048576LL);
    cb.on_update(adjusted_size);
  }
  else
    cb.on_update(cb.file_size);

  if (!into_table) {
    display_timestamps = lds->has_timestamps();
    if (display_timestamps)
      fout << "timestamp\trow\tcolumn\tvalue\n";
    else
      fout << "row\tcolumn\tvalue\n";
  }

  KeySpec key;
  ::uint8_t *value;
  ::uint32_t value_len;
  ::uint32_t consumed;
  LoadDataEscape row_escaper;
  LoadDataEscape qualifier_escaper;
  LoadDataEscape value_escaper;
  const char *escaped_buf;
  size_t escaped_len;

  try {

    while (lds->next(0, &key, &value, &value_len, &consumed)) {

      ++cb.total_cells;
      cb.total_values_size += value_len;
      cb.total_keys_size += key.row_len;

      if (state.escape) {
        row_escaper.unescape((const char *)key.row, (size_t)key.row_len,
            &escaped_buf, &escaped_len);
        key.row = escaped_buf;
        key.row_len = escaped_len;
        qualifier_escaper.unescape(key.column_qualifier, (size_t)key.column_qualifier_len,
            &escaped_buf, &escaped_len);
        key.column_qualifier = escaped_buf;
        key.column_qualifier_len = escaped_len;
        value_escaper.unescape((const char *)value, (size_t)value_len, &escaped_buf,
            &escaped_len);
      }
      else {
        escaped_buf = (const char *)value;
        escaped_len = (size_t)value_len;
      }

      if (into_table) {
        try {
          mutator->set(key, escaped_buf, escaped_len);
        }
        catch (Exception &e) {
          do {
            mutator->show_failed(e);
          } while (!mutator->retry());
        }
      }
      else {
        if (display_timestamps)
          fout << key.timestamp << "\t" << key.row << "\t" << key.column_family << "\t"
            << escaped_buf << "\n";
        else
          fout << key.row << "\t" << key.column_family << "\t" << escaped_buf << "\n";
      }

      if (cb.normal_mode && state.input_file_src != STDIN) {
        if (largefile_mode == true) {
          new_total = last_total + consumed;
          consumed = (unsigned long)((new_total / 1048576LL) - (last_total / 1048576LL));
          last_total = new_total;
        }
        cb.on_progress(consumed);
      }
    }
  }
  catch (Exception &e) {
    HT_THROW2F(Error::HQL_BAD_LOAD_FILE_FORMAT, e,
        "line number %lld", (Lld)lds->get_current_lineno());
  }

  fout.strict_sync();

  cb.on_finish(mutator.get());
}

void
cmd_insert(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  const Cells &cells = state.inserts.get();

  table = ns->open_table(state.table_name);
  mutator = table->create_mutator();

  try {
    mutator->set_cells(cells);
  }
  catch (Exception &e) {
    do {
      mutator->show_failed(e);
    } while (!mutator->retry());
  }
  if (cb.normal_mode) {
    cb.total_cells = cells.size();

    foreach(const Cell &cell, cells) {
      cb.total_keys_size += cell.column_qualifier
          ? (strlen(cell.column_qualifier) + 1) : 0;
      cb.total_values_size += cell.value_len;
    }
  }
  cb.on_finish(mutator.get());
}

void
cmd_delete(NamespacePtr &ns, ParserState &state, HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  TablePtr table;
  TableMutatorPtr mutator;
  KeySpec key;
  char *column_qualifier;

  table = ns->open_table(state.table_name);
  mutator = table->create_mutator();

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
      ++cb.total_cells;

      key.column_family = col.c_str();
      if ((column_qualifier = (char*)strchr(col.c_str(), ':')) != 0) {
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

  cb.on_finish(mutator.get());
}

void
cmd_get_listing(NamespacePtr &ns, ParserState &state,
    HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");
  std::vector<NamespaceListing> listing;
  ns->get_listing(false, listing);
  foreach (const NamespaceListing &entry, listing) {
    if (entry.is_namespace && !state.tables_only)
      cb.on_return(entry.name + "\t(namespace)");
    else if (!entry.is_namespace)
      cb.on_return(entry.name);
  }
  cb.on_finish();
}

void
cmd_drop_table(NamespacePtr &ns, ParserState &state,
               HqlInterpreter::Callback &cb) {
  if (!ns)
    HT_THROW(Error::BAD_NAMESPACE, "Null namespace");

  ns->drop_table(state.table_name, state.if_exists);
  cb.on_finish();
}

void cmd_shutdown(Client *client, HqlInterpreter::Callback &cb) {
  client->close();
  client->shutdown();
  cb.on_finish();
}

void cmd_close(Client *client, HqlInterpreter::Callback &cb) {
  client->close();
  cb.on_finish();
}

} // local namespace


HqlInterpreter::HqlInterpreter(Client *client, ConnectionManagerPtr &conn_manager,
    bool immutable_namespace) : m_client(client), m_mutator_flags(0),
    m_conn_manager(conn_manager), m_dfs_client(0), m_immutable_namespace(immutable_namespace) {
  if (Config::properties->get_bool("Hypertable.HqlInterpreter.Mutator.NoLogSync"))
    m_mutator_flags = TableMutator::FLAG_NO_LOG_SYNC;

}

void HqlInterpreter::set_namespace(const String &ns) {
  if (m_namespace && m_immutable_namespace)
    HT_THROW(Error::BAD_NAMESPACE, (String)"Attempting to modify immutable namespace " +
             m_namespace->get_name() + " to " + ns);
  m_namespace = m_client->open_namespace(ns);
}

void HqlInterpreter::execute(const String &line, Callback &cb) {
  ParserState state;
  String stripped_line = line;

  boost::trim(stripped_line);

  Hql::Parser parser(state);
  parse_info<> info = parse(stripped_line.c_str(), parser, space_p);

  if (info.full) {
    cb.on_parsed(state);

    switch (state.command) {
    case COMMAND_SHOW_CREATE_TABLE:
      cmd_show_create_table(m_namespace, state, cb);               break;
    case COMMAND_HELP:
      cmd_help(state, cb);                                         break;
    case COMMAND_EXISTS_TABLE:
      cmd_exists_table(m_namespace, state, cb);                    break;
    case COMMAND_CREATE_TABLE:
      cmd_create_table(m_namespace, state, cb);                    break;
    case COMMAND_DESCRIBE_TABLE:
      cmd_describe_table(m_namespace, state, cb);                  break;
    case COMMAND_SELECT:
      cmd_select(m_namespace, m_conn_manager, m_dfs_client,
                 state, cb);                                       break;
    case COMMAND_LOAD_DATA:
      cmd_load_data(m_namespace, m_mutator_flags,
                    m_conn_manager, m_dfs_client, state, cb);      break;
    case COMMAND_INSERT:
      cmd_insert(m_namespace, state, cb);                          break;
    case COMMAND_DELETE:
      cmd_delete(m_namespace, state, cb);                          break;
    case COMMAND_GET_LISTING:
      cmd_get_listing(m_namespace, state, cb);                     break;
    case COMMAND_ALTER_TABLE:
      cmd_alter_table(m_namespace, state, cb);                     break;
    case COMMAND_DROP_TABLE:
      cmd_drop_table(m_namespace, state, cb);                      break;
    case COMMAND_RENAME_TABLE:
      cmd_rename_table(m_namespace, state, cb);                    break;
    case COMMAND_DUMP_TABLE:
      cmd_dump_table(m_namespace, m_conn_manager, m_dfs_client,
                     state, cb);                                   break;
    case COMMAND_CLOSE:
      cmd_close(m_client, cb);                                     break;
    case COMMAND_SHUTDOWN:
      cmd_shutdown(m_client, cb);                                  break;
    case COMMAND_CREATE_NAMESPACE:
      cmd_create_namespace(m_client, m_namespace, state, cb);      break;
    case COMMAND_USE_NAMESPACE:
      cmd_use_namespace(m_client, m_namespace,
                        m_immutable_namespace, state, cb);         break;
    case COMMAND_DROP_NAMESPACE:
      cmd_drop_namespace(m_client, m_namespace, state, cb);        break;

    default:
      HT_THROW(Error::HQL_PARSE_ERROR, String("unsupported command: ") + stripped_line);
    }
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop + " (" + stripped_line + ")");
}
