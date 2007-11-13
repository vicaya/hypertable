/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

extern "C" {
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
}

#include "FileUtils.h"

using namespace Hypertable;
using namespace std;

namespace {
  bool ConvertFile(const char *fname, string &convertedContents);
}



/**
 * 
 */
int main(int argc, char **argv) {
  string convertedContents;
  string origName;
  string backupName;

  for (int i=1; i<argc; i++) {
    cout << "Converting '" << argv[i] << "'" << endl << flush;
    if (!ConvertFile(argv[i], convertedContents))
      cerr << "Problem converting '" << argv[i] << "'" << endl;
    else {
      origName = argv[i];
      backupName = (string)argv[i] + ".old";
      unlink(backupName.c_str());
      rename(origName.c_str(), backupName.c_str());
      {
	ofstream fout(argv[i]);
	fout << convertedContents;
	fout.close();
      }
    }
  }

}

namespace {

  pair<const char *, const char *> exchangeStrings[] = {
    std::make_pair("Add(", "add("),
    std::make_pair("AdvanceDataPtr", "advance_data_ptr"),
    std::make_pair("AppendByte", "append_byte"),
    std::make_pair("AppendByteArray", "append_byte_array"),
    std::make_pair("AppendBytes", "append_bytes"),
    std::make_pair("AppendInt", "append_int"),
    std::make_pair("AppendLong", "append_long"),
    std::make_pair("AppendShort", "append_short"),
    std::make_pair("AppendString", "append_string"),
    std::make_pair("CloseSocket(", "close_socket("),
    std::make_pair("Connect(", "connect("),
    std::make_pair("ConnectSocket(", "connect_socket("),
    std::make_pair("ContainsHandler(", "contains_handler("),
    std::make_pair("CreateDatagramReceiveSocket(", "create_datagram_receive_socket("),
    std::make_pair("DecomissionAll(", "decomission_all("),
    std::make_pair("DecomissionHandler(", "decomission_handler("),
    std::make_pair("GetComm(", "get_comm("),
    std::make_pair("GetDataPtr(", "get_data_ptr("),
    std::make_pair("GetDataPtrAddress(", "get_data_ptr_address("),
    std::make_pair("GetLocalAddress(", "get_local_address("),
    std::make_pair("GetThreadGroup", "get_thread_group"),
    std::make_pair("InsertDatagramHandler(", "insert_datagram_handler("),
    std::make_pair("Join(", "join("),
    std::make_pair("Listen(", "listen("),
    std::make_pair("LookupAcceptHandler(", "lookup_accept_handler("),
    std::make_pair("LookupDataHandler(", "lookup_data_handler("),
    std::make_pair("LookupDatagramHandler(", "lookup_datagram_handler("),
    std::make_pair("PurgeHandler(", "purge_handler("),
    std::make_pair("Remove(", "remove("),
    std::make_pair("RemoveHandler(", "remove_handler("),
    std::make_pair("ResetDataPointers", "reset_data_pointers"),
    std::make_pair("SendDatagram(", "send_datagram("),
    std::make_pair("SendRequest(", "send_request("),
    std::make_pair("SendResponse(", "send_response("),
    std::make_pair("SetQuietMode", "set_quiet_mode"),
    std::make_pair("SetTimer(", "set_timer("),
    std::make_pair("SetTimerAbsolute(", "set_timer_absolute("),
    std::make_pair("Shutdown(", "shutdown("),
    std::make_pair("WaitForConnection(", "wait_for_connection("),
    std::make_pair("WaitForEmpty(", "wait_for_empty("),
    std::make_pair("WaitForReply(", "wait_for_reply("),
    std::make_pair("m_app_name", "m_app_name"),
    std::make_pair("m_cond", "m_cond"),
    std::make_pair("m_datagram_handler_map", "m_datagram_handler_map"),
    std::make_pair("m_decomissioned_handlers", "m_decomissioned_handlers"),
    std::make_pair("m_event_ptr", "m_event_ptr"),
    std::make_pair("m_handler_map", "m_handler_map"),
    std::make_pair("m_impl", "m_impl"),
    std::make_pair("m_mutex", "m_mutex"),
    std::make_pair("m_receive_queue", "m_receive_queue"),
    std::make_pair("m_timer_reactor", "m_timer_reactor"),
    std::make_pair("newInstance(", "get_instance("),
    std::make_pair("toString(", "to_string("),
    std::make_pair("InitializeFromRequest(", "initialize_from_request("),
    std::make_pair("HeaderLength(", "header_length("),
    std::make_pair("Encode(", "encode("),
    std::make_pair("AssignUniqueId(", "assign_unique_id("),
    std::make_pair("SetFlags(", "set_flags("),
    std::make_pair("AddFlag(", "add_flag("),
    std::make_pair("SetProtocol(", "set_protocol("),
    std::make_pair("SetGroupId(", "set_group_id("),
    std::make_pair("SetTotalLen(", "set_total_len("),
    std::make_pair("m_id", "m_id"),
    std::make_pair("m_group_id", "m_group_id"),
    std::make_pair("m_total_len", "m_total_len"),
    std::make_pair("m_protocol", "m_protocol"),
    std::make_pair("m_flags", "m_flags"),
    std::make_pair((const char *)0, (const char *)0)
  };

  typedef struct {
    off_t offset;
    const char *fromString;
    const char *toString;
  } ReplaceInfoT;

  struct ltReplaceInfo {
    bool operator()(const ReplaceInfoT &ri1, const ReplaceInfoT &ri2) const {
      return ri1.offset < ri2.offset;
    }
  };

  const char *commentHeader = \
  "/**\n"
  " * Copyright (C) 2007 Doug Judd (Zvents, Inc.)\n"
  " * \n"
  " * This file is part of Hypertable.\n"
  " * \n"
  " * Hypertable is free software; you can redistribute it and/or\n"
  " * modify it under the terms of the GNU General Public License\n"
  " * as published by the Free Software Foundation; either version 2\n"
  " * of the License, or any later version.\n"
  " * \n"
  " * Hypertable is distributed in the hope that it will be useful,\n"
  " * but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
  " * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
  " * GNU General Public License for more details.\n"
  " * \n"
  " * You should have received a copy of the GNU General Public License\n"
  " * along with this program; if not, write to the Free Software\n"
  " * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.\n"
  " */";

  bool ConvertFile(const char *fname, string &convertedContents) {
    off_t lastOffset, flen;
    char  *fcontents = FileUtils::file_to_buffer(fname, &flen);
    char *ptr = fcontents;
    char *codeStart, *base;
    ReplaceInfoT rinfo;
    vector<ReplaceInfoT> riVec;

    convertedContents = commentHeader;

    if (fcontents == 0)
      return false;

#if 0
    while (*ptr && isspace(*ptr))
      ptr++;

    if (strncmp(ptr, "/*", 2)) {
      cerr << "Comment header not found in '" << fname << "'" << endl;
      return false;
    }

    if ((ptr = strstr(ptr, "*/")) == 0) {
      cerr << "Unable to find end of comment header in '" << fname << "'" << endl;
      return false;
    }

    codeStart = ptr + 2;
#endif

    codeStart = ptr;

    for (size_t i=0; exchangeStrings[i].first != 0; i++) {
      base = codeStart;
      while ((ptr = strstr(base, exchangeStrings[i].first)) != 0) {
	rinfo.offset = ptr - fcontents;
	rinfo.fromString = exchangeStrings[i].first;
	rinfo.toString = exchangeStrings[i].second;
	riVec.push_back(rinfo);
	base = ptr + strlen(exchangeStrings[i].first);
      }
    }

    if (!riVec.empty()) {
      ltReplaceInfo ltObj;      
      sort(riVec.begin(), riVec.end(), ltObj);
    }

    lastOffset = codeStart - fcontents;
    for (size_t i=0; i<riVec.size(); i++) {
      cerr << "Replace at offset " << riVec[i].offset << endl << flush;
      convertedContents += string(fcontents + lastOffset, riVec[i].offset-lastOffset);
      convertedContents += riVec[i].toString;
      lastOffset = riVec[i].offset + strlen(riVec[i].fromString);
    }

    convertedContents += string(fcontents + lastOffset, flen-lastOffset);

    return true;
  }

}


