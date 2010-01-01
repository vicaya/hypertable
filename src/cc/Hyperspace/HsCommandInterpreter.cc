/** -*- c++ -*-
 * Copyright (C) 2008 Sanjit Jhala (Zvents, Inc.)
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

extern "C" {
#include <time.h>
#if defined(__sun__)
#include <inttypes.h>
#endif
}


#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"
#include "Common/DynamicBuffer.h"
#include "HsCommandInterpreter.h"
#include "HsHelpText.h"
#include "HsParser.h"
#include "DirEntry.h"
#include "FileHandleCallback.h"

using namespace std;
using namespace Hyperspace;
using namespace HsParser;


HsCommandInterpreter::HsCommandInterpreter(Session* session)
    : m_session(session) {
}


void HsCommandInterpreter::execute_line(const String &line) {
  String out_str;
  ParserState state;
  Parser parser(state);
  parse_info<> info;

  info = parse(line.c_str(), parser, space_p);

  if (info.full) {

    if (state.command == COMMAND_MKDIR) {
      String fname = state.dir_name;
      m_session->mkdir(fname);
    }

    else if (state.command == COMMAND_DELETE) {
      String fname = state.node_name;
      m_session->unlink(fname);
    }

    else if (state.command == COMMAND_OPEN) {
      ::uint64_t handle;
      int event_mask = state.event_mask;
      int open_flag = state.open_flag;
      String fname = state.node_name;

      if(open_flag == 0) // read is default
        open_flag = OPEN_FLAG_READ;

      if(fname == "")
        HT_THROW(Error::HYPERSPACE_CLI_PARSE_ERROR,
                 "Error: no filename supplied.");

      HandleCallbackPtr callback = new FileHandleCallback(event_mask);
      handle = m_session->open(fname,open_flag,callback);

      // store opened handle in global HsClientState namespace
      String normal_name;
      Util::normalize_pathname(fname,normal_name);
      HsClientState::file_map[normal_name] = handle;
    }

    else if (state.command == COMMAND_CREATE) {
      ::uint64_t handle;
      int event_mask = state.event_mask;
      int open_flag = state.open_flag;
      String fname = state.file_name;

      if(open_flag == 0)
        HT_THROW(Error::HYPERSPACE_CLI_PARSE_ERROR,
                 "Error: no flags supplied.");
      else if (fname == "")
        HT_THROW(Error::HYPERSPACE_CLI_PARSE_ERROR,
                 "Error: no filename supplied.");

      HandleCallbackPtr callback = new FileHandleCallback(event_mask);
      handle = m_session->create(fname,open_flag,callback,state.attrs);

      // store opened handle in global HsClientState namespace
      String normal_name;
      Util::normalize_pathname(fname,normal_name);
      HsClientState::file_map[normal_name] = handle;
    }

    else if (state.command == COMMAND_CLOSE) {
      ::uint64_t handle;
      String fname = state.node_name;
      handle = Util::get_handle(fname);
      String normal_name;
      Util::normalize_pathname(fname,normal_name);
      HsClientState::file_map.erase(normal_name);
      m_session->close(handle);
    }

    else if (state.command == COMMAND_ATTRSET) {
      ::uint64_t handle;
      int size = state.last_attr_size;
      String name = state.last_attr_name;
      String value = state.last_attr_value;
      String fname = state.node_name;
      const char *val = value.c_str();

      handle = Util::get_handle(fname);

      m_session->attr_set(handle, name, val, size);
    }

    else if (state.command == COMMAND_ATTRGET) {
      ::uint64_t handle;
      String name = state.last_attr_name;
      String fname = state.node_name;
      DynamicBuffer value(0);

      handle = Util::get_handle(fname);

      m_session->attr_get(handle, name, value);

     String valstr = String((const char*)(value.base),value.fill());
     cout << valstr << endl;

    }

    else if (state.command == COMMAND_ATTREXISTS) {
      ::uint64_t handle;
      String name = state.last_attr_name;
      String fname = state.node_name;
      bool exists;

      handle = Util::get_handle(fname);

      exists = m_session->attr_exists(handle, name);

      if (exists)
        cout << "true" << endl;
      else
        cout << "false" << endl;
    }

    else if (state.command == COMMAND_ATTRLIST) {
      ::uint64_t handle;
      String fname = state.node_name;
      vector<String> attrs;

      handle = Util::get_handle(fname);

      m_session->attr_list(handle, attrs);

      for (vector<String>::iterator it = attrs.begin(); it != attrs.end(); ++it) {
        cout << *it << endl;
      }
    }

    else if (state.command == COMMAND_ATTRDEL) {
      ::uint64_t handle;
      String name = state.last_attr_name;
      String fname = state.node_name;

      handle = Util::get_handle(fname);

      m_session->attr_del(handle, name);
    }

    else if (state.command == COMMAND_EXISTS) {
      String fname = state.node_name;

      if(m_session->exists(fname)) {
        cout<< "true" << endl;
      }
      else {
        cout << "false" << endl;
        HsClientState::exit_status = 1;
      }
    }

    else if (state.command == COMMAND_READDIR) {
      ::uint64_t handle;
      vector<struct DirEntry> listing;
      String fname = state.dir_name;

      handle = Util::get_handle(fname);
      m_session->readdir(handle, listing);

      struct LtDirEntry ascending;
      sort(listing.begin(), listing.end(), ascending);
      for (size_t ii=0; ii<listing.size(); ii++) {
        if (listing[ii].is_dir)
          cout << "(dir) ";
        else
          cout << "      ";
        cout << listing[ii].name << endl ;
      }
      cout << flush ;
    }

    else if (state.command == COMMAND_LOCK) {
      ::uint64_t handle;
      ::uint32_t mode = state.lock_mode;
      String fname = state.node_name;
      struct LockSequencer lockseq;

      handle = Util::get_handle(fname);

      m_session->lock(handle, mode, &lockseq);

      cout << "SEQUENCER name=" << lockseq.name << " mode=" << lockseq.mode
           << " generation=" << lockseq.generation << endl;
    }

    else if (state.command == COMMAND_TRYLOCK) {
      ::uint64_t handle;
      ::uint32_t mode = state.lock_mode;
      String fname = state.node_name;
      struct LockSequencer lockseq;
      ::uint32_t status;

      handle = Util::get_handle(fname);
      m_session->try_lock(handle, mode, &status, &lockseq);

      if (status == LOCK_STATUS_GRANTED)
        cout << "SEQUENCER name=" << lockseq.name << " mode=" << lockseq.mode
             << " generation=" << lockseq.generation << endl;
      else if (status == LOCK_STATUS_BUSY)
        cout << "busy" << endl;
      else
        cout << "Unknown status: " << status << endl;

    }

    else if (state.command == COMMAND_RELEASE) {
      ::uint64_t handle;
      String fname = state.node_name;

      handle = Util::get_handle(fname);

      m_session->release(handle);
    }

    else if (state.command == COMMAND_GETSEQ) {
      ::uint64_t handle;
      struct LockSequencer lockseq;
      String fname = state.node_name;

      handle = Util::get_handle(fname);

      m_session->get_sequencer(handle, &lockseq);

      cout << "SEQUENCER name=" << lockseq.name << " mode=" << lockseq.mode
           << " generation=" << lockseq.generation << endl;
    }

    else if (state.command == COMMAND_ECHO) {
      String str = state.str;

      cout << str << endl;
    }

    else if (state.command == COMMAND_LOCATE) {
      int type = state.locate_type;
      String result = m_session->locate(type);
      cout << result << endl;
    }

    else if (state.command == COMMAND_HELP) {
      const char **text = HsHelpText::get(state.help_str);

      if (text) {
        for (size_t i=0; text[i]; i++)
          cout << text[i] << endl;
      }
      else
        cout << endl << "no help for '" << state.help_str << "'\n" << endl;
    }

    else
      HT_THROW(Error::HYPERSPACE_CLI_PARSE_ERROR, "unsupported command: "
               + line);
  }
  else
    HT_THROWF(Error::HYPERSPACE_CLI_PARSE_ERROR, "parse error at: %s",
              info.stop);
}
