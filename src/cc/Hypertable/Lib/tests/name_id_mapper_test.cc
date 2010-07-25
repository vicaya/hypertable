/** -*- C++ -*-
 * Copyright (C) 2009  Luke Lu (llu@hypertable.org)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"
#include "Common/Init.h"

#include <unistd.h>

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/NameIdMapper.h"
#include "Hyperspace/Session.h"
#include "Hyperspace/Config.h"

#include "AsyncComm/Comm.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace Config;
using namespace std;

namespace {

  String namemap_ids_dir = (String)"/hypertable/namemap/ids/";
  String namemap_names_dir = (String)"/hypertable/namemap/names/";
  String ns1_full_name = namemap_names_dir + "ns1";
  String ns1_full_id= namemap_ids_dir + "1";
  String ns1_last_name = (String)"ns1";
  String ns1_last_id= (String)"1";
  String t1_full_name = namemap_names_dir + "ns1/t1";
  String t1_full_id= namemap_ids_dir + "1/0";
  String t1_last_name = (String)"t1";
  String t1_last_id= (String)"0";
  String ns2_full_name = namemap_names_dir + "ns1/ns2";
  String ns2_full_id= namemap_ids_dir + "1/1";
  String ns2_last_name = (String)"ns2";
  String ns2_last_id= (String)"1";
  String t2_full_name = namemap_names_dir + "ns1/ns2/t2";
  String t2_full_id= namemap_ids_dir + "1/1/0";
  String t2_last_name = (String)"t2";
  String t2_last_id= (String)"0";


void create_mapping(Hyperspace::SessionPtr session, String &full_name, String &last_name,
                    String &full_id, String &last_id, bool is_namespace) {
  uint32_t oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_LOCK;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;

  // Create name file/dir and set id attr
  if (is_namespace)
    session->mkdir(full_name);
  handle = session->open(full_name, oflags, null_handle_callback);
  session->attr_set(handle, "id", (const void*)last_id.c_str(), last_id.size());
  session->close(handle);

  //Create name file/dir and set id attr
  if (is_namespace)
    session->mkdir(full_id);
  handle = session->open(full_id, oflags, null_handle_callback);
  session->attr_set(handle, "name", (const void*)last_name.c_str(), last_name.size());
  session->close(handle);
}

void init(Hyperspace::SessionPtr &session) {
  create_mapping(session, ns1_full_name, ns1_last_name , ns1_full_id , ns1_last_id, true);
  create_mapping(session, t1_full_name , t1_last_name, t1_full_id , t1_last_id, false);
  create_mapping(session, ns2_full_name, ns2_last_name , ns2_full_id , ns2_last_id, true);
  create_mapping(session, t2_full_name , t2_last_name, t2_full_id , t2_last_id, false);
}

void test_mapper(Hyperspace::SessionPtr &session) {

  NameIdMapper mapper(session);
  String output;

  HT_ASSERT(mapper.name_to_id("ns1/ns2/t2", output));
  HT_ASSERT(output == (String)"1/1/0");
  HT_ASSERT(mapper.id_to_name("/1/1/", output));
  HT_ASSERT(output == (String)"/ns1/ns2/");
  HT_ASSERT(!mapper.id_to_name("/1/should_not_exist/", output));
  HT_ASSERT(!mapper.id_to_name("/ns1/should_not_exist", output));
}

void cleanup(Hyperspace::SessionPtr &session) {
  session->unlink(t2_full_name );
  session->unlink(t2_full_id );
  session->unlink(ns2_full_name);
  session->unlink(ns2_full_id);
  session->unlink(t1_full_name);
  session->unlink(t1_full_id);
  session->unlink(ns1_full_name);
  session->unlink(ns1_full_id);
}

} // local namesapce


int main(int argc, char *argv[]) {
  typedef Cons<DefaultClientPolicy, DefaultCommPolicy> MyPolicy;

  try {
    init_with_policy<MyPolicy>(argc, argv);

    Comm *comm = Comm::instance();
    SessionPtr session = new Hyperspace::Session(comm, properties);

    if (!session->exists("/hypertable"))
      session->mkdir("/hypertable");
    if (!session->exists("/hypertable/namemap"))
      session->mkdir("/hypertable/namemap");
    if (!session->exists("/hypertable/namemap/ids"))
      session->mkdir("/hypertable/namemap/ids");
    if (!session->exists("/hypertable/namemap/names"))
      session->mkdir("/hypertable/namemap/names");

    init(session);
    test_mapper(session);
    cleanup(session);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  _exit(0);
}
