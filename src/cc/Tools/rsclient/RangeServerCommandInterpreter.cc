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
#include <cassert>
#include <cstdio>
#include <cstring>

#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
}

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Common/Error.h"
#include "Common/FileUtils.h"

#include "Hypertable/Lib/HqlHelpText.h"
#include "Hypertable/Lib/HqlParser.h"
#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/LoadDataSource.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/ScanBlock.h"
#include "Hypertable/Lib/ScanSpec.h"
#include "Hypertable/Lib/TestSource.h"

#include "RangeServerCommandInterpreter.h"

#define BUFFER_SIZE 65536

using namespace std;
using namespace Hypertable;
using namespace HqlParser;
using namespace Serialization;

namespace {

  void process_event(EventPtr &event_ptr) {
    int32_t error;
    const uint8_t *ptr = event_ptr->message + 4;
    size_t remaining = event_ptr->message_len - 4;
    uint32_t offset, len;
    if (remaining == 0)
      cout << "success" << endl;
    else {
      while (remaining) {
        try {
          error = decode_i32(&ptr, &remaining);
          offset = decode_i32(&ptr, &remaining);
          len = decode_i32(&ptr, &remaining);
        }
        catch (Exception &e) {
          HT_ERROR_OUT << e << HT_END;
          break;
        }
        cout << "rejected: offset=" << offset << " span=" << len << " " << Error::get_text(error) << endl;
      }
    }
  }

}

RangeServerCommandInterpreter::RangeServerCommandInterpreter(Comm *comm, Hyperspace::SessionPtr &hyperspace_ptr, struct sockaddr_in addr, RangeServerClientPtr &range_server_ptr) : m_comm(comm), m_hyperspace_ptr(hyperspace_ptr), m_addr(addr), m_range_server_ptr(range_server_ptr), m_cur_scanner_id(-1) {
  HqlHelpText::install_range_server_client_text();
  return;
}


void RangeServerCommandInterpreter::execute_line(const String &line) {
  TableIdentifier *table = 0;
  RangeSpec range;
  TableInfo *table_info;
  std::string schema_str;
  std::string out_str;
  SchemaPtr schema_ptr;
  hql_interpreter_state state;
  hql_interpreter interp(state);
  parse_info<> info;
  DispatchHandlerSynchronizer sync_handler;
  ScanBlock scanblock;
  int32_t scanner_id;
  EventPtr event_ptr;

  info = parse(line.c_str(), interp, space_p);

  if (info.full) {

    // if table name specified, get associated objects
    if (state.table_name != "") {
      table_info = m_table_map[state.table_name];
      if (table_info == 0) {
        table_info = new TableInfo(state.table_name);
        table_info->load(m_hyperspace_ptr);
	m_table_map[state.table_name] = table_info;
      }
      table = table_info->get_table_identifier();
      table_info->get_schema_ptr(schema_ptr);
    }

    // if end row is "??", transform it to 0xff 0xff
    if (state.range_end_row == "??")
      state.range_end_row = Key::END_ROW_MARKER;

    if (state.command == COMMAND_LOAD_RANGE) {
      RangeState range_state;

      cout << "TableName  = " << state.table_name << endl;
      cout << "StartRow   = " << state.range_start_row << endl;
      cout << "EndRow     = " << state.range_end_row << endl;

      range.start_row = state.range_start_row.c_str();
      range.end_row = state.range_end_row.c_str();

      range_state.soft_limit = 200000000LL;

      m_range_server_ptr->load_range(m_addr, *table, range, 0, range_state, 0);

    }
    else if (state.command == COMMAND_UPDATE) {
      TestSource *tsource = 0;

      if (!FileUtils::exists(state.input_file.c_str()))
        HT_THROW(Error::FILE_NOT_FOUND, std::string("Input file '") + state.input_file + "' does not exist");

      tsource = new TestSource(state.input_file, schema_ptr.get());

      uint8_t *send_buf = 0;
      size_t   send_buf_len = 0;
      DynamicBuffer buf(BUFFER_SIZE);
      ByteString key, value;
      size_t key_len, value_len;
      bool outstanding = false;

      while (true) {

        while (tsource->next(key, value)) {
          key_len = key.length();
          value_len = value.length();
          buf.ensure(key_len + value_len);
          buf.add_unchecked(key.ptr, key_len);
          buf.add_unchecked(value.ptr, value_len);
          if (buf.fill() > BUFFER_SIZE)
            break;
        }

        /**
         * Sort the keys
         */
        if (buf.fill()) {
          std::vector<ByteString> keys;
          struct LtByteString ltbs;
          const uint8_t *ptr;
          size_t len;

          key.ptr = ptr = buf.base;

          while (ptr < buf.ptr) {
            keys.push_back(key);
            key.next();
            key.next();
            ptr = key.ptr;
          }

          std::sort(keys.begin(), keys.end(), ltbs);

          send_buf = new uint8_t [buf.fill()];
          uint8_t *sendp = send_buf;
          for (size_t i=0; i<keys.size(); i++) {
            key = keys[i];
            key.next();
            key.next();
            len = key.ptr - keys[i].ptr;
            memcpy(sendp, keys[i].ptr, len);
            sendp += len;
          }
          send_buf_len = sendp - send_buf;
          buf.clear();
        }
        else
          send_buf_len = 0;

        if (outstanding) {
          if (!sync_handler.wait_for_reply(event_ptr))
            HT_THROW(Protocol::response_code(event_ptr), (Protocol::string_format_message(event_ptr)));
          process_event(event_ptr);
        }

        outstanding = false;

        if (send_buf_len > 0) {
          StaticBuffer mybuf(send_buf, send_buf_len);
          m_range_server_ptr->update(m_addr, *table, mybuf, &sync_handler);
          outstanding = true;
        }
        else
          break;
      }

      if (outstanding) {
        if (!sync_handler.wait_for_reply(event_ptr))
          HT_THROW(Protocol::response_code(event_ptr), (Protocol::string_format_message(event_ptr)));
        process_event(event_ptr);
      }

    }
    else if (state.command == COMMAND_CREATE_SCANNER) {
      ScanSpec scan_spec;
      RowInterval ri;

      range.start_row = state.range_start_row.c_str();
      range.end_row = state.range_end_row.c_str();

      /**
       * Create Scan specification
       */
      scan_spec.row_limit = state.scan.limit;
      scan_spec.max_versions = state.scan.max_versions;
      for (size_t i=0; i<state.scan.columns.size(); i++)
        scan_spec.columns.push_back(state.scan.columns[i].c_str());

      for (size_t i=0; i<state.scan.row_intervals.size(); i++) {
	if (state.scan.row_intervals[i].start > state.scan.row_intervals[i].end ||
	    (state.scan.row_intervals[i].start == state.scan.row_intervals[i].end &&
	     !(state.scan.row_intervals[i].start_inclusive || state.scan.row_intervals[i].end_inclusive)))
	  HT_THROW(Error::HQL_PARSE_ERROR, "Bad row range");
	ri.start = (state.scan.row_intervals[i].start == "") ? ""
	  : state.scan.row_intervals[i].start.c_str();
	ri.start_inclusive = state.scan.row_intervals[i].start_inclusive;
	ri.end = state.scan.row_intervals[i].end.c_str();
	ri.end_inclusive = state.scan.row_intervals[i].end_inclusive;
	scan_spec.row_intervals.push_back(ri);
      }

      scan_spec.time_interval.first  = state.scan.start_time;
      scan_spec.time_interval.second = state.scan.end_time;
      scan_spec.return_deletes = state.scan.return_deletes;

      /**
       */
      m_range_server_ptr->create_scanner(m_addr, *table, range, scan_spec, scanblock);

      m_cur_scanner_id = scanblock.get_scanner_id();

      ByteString key, value;

      while (scanblock.next(key, value))
        display_scan_data(key, value, schema_ptr);

      if (scanblock.eos())
        m_cur_scanner_id = -1;

    }
    else if (state.command == COMMAND_FETCH_SCANBLOCK) {

      if (state.scanner_id == -1) {
        if (m_cur_scanner_id == -1)
          HT_THROW(Error::RANGESERVER_INVALID_SCANNER_ID, "No currently open scanner");
        scanner_id = m_cur_scanner_id;
        m_cur_scanner_id = -1;
      }
      else
        scanner_id = state.scanner_id;

      /**
       */
      m_range_server_ptr->fetch_scanblock(m_addr, scanner_id, scanblock);

      ByteString key, value;

      while (scanblock.next(key, value))
        display_scan_data(key, value, schema_ptr);

      if (scanblock.eos())
        m_cur_scanner_id = -1;

    }
    else if (state.command == COMMAND_DESTROY_SCANNER) {

      if (state.scanner_id == -1) {
        if (m_cur_scanner_id == -1)
          return;
        scanner_id = m_cur_scanner_id;
        m_cur_scanner_id = -1;
      }
      else
        scanner_id = state.scanner_id;

      m_range_server_ptr->destroy_scanner(m_addr, scanner_id);

    }
    else if (state.command == COMMAND_DROP_RANGE) {

      range.start_row = state.range_start_row.c_str();
      range.end_row = state.range_end_row.c_str();

      m_range_server_ptr->drop_range(m_addr, *table, range, &sync_handler);

      if (!sync_handler.wait_for_reply(event_ptr))
        HT_THROW(Protocol::response_code(event_ptr), (Protocol::string_format_message(event_ptr)));

    }
    else if (state.command == COMMAND_REPLAY_BEGIN) {
      m_range_server_ptr->replay_begin(m_addr, false, &sync_handler);  // fix me!!  added metadata boolean
      if (!sync_handler.wait_for_reply(event_ptr))
        HT_THROW(Protocol::response_code(event_ptr), (Protocol::string_format_message(event_ptr)));
    }
    else if (state.command == COMMAND_REPLAY_LOG) {
      cout << "Not implemented." << endl;
    }
    else if (state.command == COMMAND_REPLAY_COMMIT) {
      m_range_server_ptr->replay_commit(m_addr, &sync_handler);
      if (!sync_handler.wait_for_reply(event_ptr))
        HT_THROW(Protocol::response_code(event_ptr), (Protocol::string_format_message(event_ptr)));
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
    else if (state.command == COMMAND_SHUTDOWN) {
      m_range_server_ptr->shutdown(m_addr);
    }
    else
      HT_THROW(Error::HQL_PARSE_ERROR, "unsupported command");
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, std::string("parse error at: ") + info.stop);
}



/**
 *
 */
void
RangeServerCommandInterpreter::display_scan_data(const ByteString &bskey,
    const ByteString &value, SchemaPtr &schema_ptr) {
  Key key(bskey);
  Schema::ColumnFamily *cf;

  if (key.flag == FLAG_DELETE_ROW) {
    cout << key.timestamp << " " << key.row << " DELETE" << endl;
  }
  else {
    if (key.column_family_code > 0) {
      cf = schema_ptr->get_column_family(key.column_family_code);
      if (key.flag == FLAG_DELETE_CELL)
        cout << key.timestamp << " " << key.row << " " << cf->name << ":" << key.column_qualifier << " DELETE" << endl;
      else {
        cout << key.timestamp << " " << key.row << " " << cf->name << ":" << key.column_qualifier;
        cout << endl;
      }
    }
    else {
      cerr << "Bad column family (" << (int)key.column_family_code << ") for row key " << key.row;
      cerr << endl;
    }
  }
}
