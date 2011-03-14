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
#include "Common/Init.h"

#include <cassert>
#include <cstdio>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/progress.hpp>
#include <boost/thread/xtime.hpp>
#include <boost/timer.hpp>

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
using namespace Hypertable::Config;
using namespace Hql;
using namespace Serialization;

namespace {

  void process_event(EventPtr &event) {
    ::int32_t error;
    const ::uint8_t *decode_ptr = event->payload + 4;
    size_t decode_remain = event->payload_len - 4;
    ::uint32_t offset, len;
    if (decode_remain == 0)
      cout << "success" << endl;
    else {
      while (decode_remain) {
        try {
          error = decode_i32(&decode_ptr, &decode_remain);
          offset = decode_i32(&decode_ptr, &decode_remain);
          len = decode_i32(&decode_ptr, &decode_remain);
        }
        catch (Exception &e) {
          HT_ERROR_OUT << e << HT_END;
          break;
        }
        cout << "rejected: offset=" << offset << " span=" << len << " "
             << Error::get_text(error) << endl;
      }
    }
  }

}

RangeServerCommandInterpreter::RangeServerCommandInterpreter(Comm *comm,
    Hyperspace::SessionPtr &hyperspace, const sockaddr_in addr,
    RangeServerClientPtr &range_server)
  : m_comm(comm), m_hyperspace(hyperspace), m_addr(addr),
    m_range_server(range_server), m_cur_scanner_id(-1) {
  HqlHelpText::install_range_server_client_text();
  if (m_hyperspace) {
    m_toplevel_dir = properties->get_str("Hypertable.Directory");
    boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
    m_toplevel_dir = String("/") + m_toplevel_dir;
    m_namemap = new NameIdMapper(m_hyperspace, m_toplevel_dir);
  }
  return;
}


void RangeServerCommandInterpreter::execute_line(const String &line) {
  TableIdentifier *table = 0;
  RangeSpec range;
  TableInfo *table_info;
  String schema_str;
  String out_str;
  SchemaPtr schema;
  Hql::ParserState state;
  Hql::Parser parser(state);
  parse_info<> info;
  DispatchHandlerSynchronizer sync_handler;
  ScanBlock scanblock;
  ::int32_t scanner_id;
  EventPtr event;

  info = parse(line.c_str(), parser, space_p);

  if (info.full) {

    // if table name specified, get associated objects
    if (state.table_name != "") {
      table_info = m_table_map[state.table_name];
      if (table_info == 0) {
        bool is_namespace = false;
        String table_id;
        if (!m_hyperspace)
          HT_FATALF("Hyperspace is required to execute:  %s", line.c_str());
        if (!m_namemap->name_to_id(state.table_name, table_id, &is_namespace) ||
            is_namespace)
          HT_THROW(Error::TABLE_NOT_FOUND, state.table_name);
        table_info = new TableInfo(m_toplevel_dir, table_id);
        table_info->load(m_hyperspace);
        m_table_map[state.table_name] = table_info;
      }
      table = table_info->get_table_identifier();
      table_info->get_schema_ptr(schema);
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

      m_range_server->load_range(m_addr, *table, range, 0, range_state, 0, false);

    }
    else if (state.command == COMMAND_UPDATE) {
      TestSource *tsource = 0;

      if (!FileUtils::exists(state.input_file.c_str()))
        HT_THROW(Error::FILE_NOT_FOUND, String("Input file '")
                 + state.input_file + "' does not exist");

      tsource = new TestSource(state.input_file, schema.get());

      ::uint8_t *send_buf = 0;
      size_t   send_buf_len = 0;
      DynamicBuffer buf(BUFFER_SIZE);
      SerializedKey key;
      ByteString value;
      size_t key_len, value_len;
      ::uint32_t send_count = 0;
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
          std::vector<SerializedKey> keys;
          const ::uint8_t *ptr;
          size_t len;

          key.ptr = ptr = buf.base;

          while (ptr < buf.ptr) {
            keys.push_back(key);
            key.next();
            key.next();
            ptr = key.ptr;
          }

          std::sort(keys.begin(), keys.end());

          send_buf = new ::uint8_t [buf.fill()];
          ::uint8_t *sendp = send_buf;
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
          send_count = keys.size();
        }
        else {
          send_buf_len = 0;
          send_count = 0;
        }

        if (outstanding) {
          if (!sync_handler.wait_for_reply(event))
            HT_THROW(Protocol::response_code(event),
                     (Protocol::string_format_message(event)));
          process_event(event);
        }

        outstanding = false;

        if (send_buf_len > 0) {
          StaticBuffer mybuf(send_buf, send_buf_len);
          m_range_server->update(m_addr, *table, send_count, mybuf, 0,
                                     &sync_handler);
          outstanding = true;
        }
        else
          break;
      }

      if (outstanding) {
        if (!sync_handler.wait_for_reply(event))
          HT_THROW(Protocol::response_code(event),
                   (Protocol::string_format_message(event)));
        process_event(event);
      }

    }
    else if (state.command == COMMAND_CREATE_SCANNER) {
      range.start_row = state.range_start_row.c_str();
      range.end_row = state.range_end_row.c_str();
      m_range_server->create_scanner(m_addr, *table, range,
          state.scan.builder.get(), scanblock);
      m_cur_scanner_id = scanblock.get_scanner_id();

      SerializedKey key;
      ByteString value;

      while (scanblock.next(key, value))
        display_scan_data(key, value, schema);

      if (scanblock.eos())
        m_cur_scanner_id = -1;

    }
    else if (state.command == COMMAND_FETCH_SCANBLOCK) {

      if (state.scanner_id == -1) {
        if (m_cur_scanner_id == -1)
          HT_THROW(Error::RANGESERVER_INVALID_SCANNER_ID,
                   "No currently open scanner");
        scanner_id = m_cur_scanner_id;
        m_cur_scanner_id = -1;
      }
      else
        scanner_id = state.scanner_id;

      /**
       */
      m_range_server->fetch_scanblock(m_addr, scanner_id, scanblock);

      SerializedKey key;
      ByteString value;

      while (scanblock.next(key, value))
        display_scan_data(key, value, schema);

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

      m_range_server->destroy_scanner(m_addr, scanner_id);

    }
    else if (state.command == COMMAND_DROP_RANGE) {

      range.start_row = state.range_start_row.c_str();
      range.end_row = state.range_end_row.c_str();

      m_range_server->drop_range(m_addr, *table, range, &sync_handler);

      if (!sync_handler.wait_for_reply(event))
        HT_THROW(Protocol::response_code(event),
                 (Protocol::string_format_message(event)));

    }
    else if (state.command == COMMAND_REPLAY_BEGIN) {
      // fix me!!  added metadata boolean
      m_range_server->replay_begin(m_addr, false, &sync_handler);
      if (!sync_handler.wait_for_reply(event))
        HT_THROW(Protocol::response_code(event),
                 (Protocol::string_format_message(event)));
    }
    else if (state.command == COMMAND_REPLAY_LOG) {
      cout << "Not implemented." << endl;
    }
    else if (state.command == COMMAND_DUMP) {
      m_range_server->dump(m_addr, state.output_file, state.nokeys);
      cout << "success" << endl;
    }
    else if (state.command == COMMAND_REPLAY_COMMIT) {
      m_range_server->replay_commit(m_addr, &sync_handler);
      if (!sync_handler.wait_for_reply(event))
        HT_THROW(Protocol::response_code(event),
                 (Protocol::string_format_message(event)));
    }
    else if (state.command == COMMAND_HELP) {
      const char **text = HqlHelpText::get(state.str);
      if (text) {
        for (size_t i=0; text[i]; i++)
          cout << text[i] << endl;
      }
      else
        cout << endl << "no help for '" << state.str << "'" << endl << endl;
    }
    else if (state.command == COMMAND_CLOSE) {
      m_range_server->close(m_addr);
    }
    else if (state.command == COMMAND_WAIT_FOR_MAINTENANCE) {
      m_range_server->wait_for_maintenance(m_addr);
    }
    else if (state.command == COMMAND_SHUTDOWN) {
      m_range_server->close(m_addr);
      m_range_server->shutdown(m_addr);
    }
    else
      HT_THROW(Error::HQL_PARSE_ERROR, "unsupported command");
  }
  else
    HT_THROW(Error::HQL_PARSE_ERROR, String("parse error at: ") + info.stop);
}



/**
 *
 */
void
RangeServerCommandInterpreter::display_scan_data(const SerializedKey &serkey,
                                                 const ByteString &value,
                                                 SchemaPtr &schema) {
  Key key(serkey);
  Schema::ColumnFamily *cf;

  if (key.flag == FLAG_DELETE_ROW) {
    cout << key.timestamp << " " << key.row << " DELETE" << endl;
  }
  else if (key.flag == FLAG_DELETE_COLUMN_FAMILY) {
     cf = schema->get_column_family(key.column_family_code);
     cout << key.timestamp << " " << key.row << " " << cf->name << " DELETE"
          << endl;
  }
  else {
    if (key.column_family_code > 0) {
      cf = schema->get_column_family(key.column_family_code);
      if (key.flag == FLAG_DELETE_CELL)
        cout << key.timestamp << " " << key.row << " " << cf->name << ":"
             << key.column_qualifier << " DELETE" << endl;
      else {
        cout << key.timestamp << " " << key.row << " " << cf->name << ":"
             << key.column_qualifier;
        cout << endl;
      }
    }
    else {
      cerr << "Bad column family (" << (int)key.column_family_code
           << ") for row key " << key.row;
      cerr << endl;
    }
  }
}
