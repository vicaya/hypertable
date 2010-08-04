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
#include "Common/ServerLauncher.h"

#include <vector>
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

/**
 * aaa/
 * acme/
 * acme/foo/tableA
 * acme/foo/tableB
 * acme/bar/tableA
 * acme/stats
 * acme/camp/
 */

namespace {

  class Mapping {
  public:
    Mapping(const String &n, const String &i, bool ns)
      : name(n), id(i), is_namespace(ns) { }
    String name;
    String id;
    bool is_namespace;
  };

  std::vector<Mapping> mappings;

  struct LengthDescending {
    bool operator()(const Mapping &m1, const Mapping &m2) const {
      return m2.name.length() < m1.name.length();
    }
  };


void init(NameIdMapper &mapper) {
  String id;

  mapper.add_mapping("SYS", id, NameIdMapper::IS_NAMESPACE);
  HT_ASSERT(id == "0");
  mappings.push_back( Mapping("SYS", "0", true) );

  mapper.add_mapping("SYS/METADATA", id, 0);
  HT_ASSERT(id == "0/0");
  mappings.push_back( Mapping("SYS/METADATA", "0/0", false) );

  mapper.add_mapping("acme", id, NameIdMapper::IS_NAMESPACE);
  HT_ASSERT(id == "1");
  mappings.push_back( Mapping("acme", "1", true) );

  mapper.add_mapping("acme/foo", id, NameIdMapper::IS_NAMESPACE);
  HT_ASSERT(id == "1/0");
  mappings.push_back( Mapping("acme/foo", "1/0", true) );

  mapper.add_mapping("acme/foo/bar/tableC", id, NameIdMapper::CREATE_INTERMEDIATE);
  HT_ASSERT(id == "1/0/0/0");
  mappings.push_back( Mapping("acme/foo/bar/tableC", "1/0/0/0", false) );
  mappings.push_back( Mapping("acme/foo/bar", "1/0/0", true) );

  mapper.add_mapping("acme/foo/tableA", id);
  HT_ASSERT(id == "1/0/1");
  mappings.push_back( Mapping("acme/foo/tableA", "1/0/1", false) );

  mapper.add_mapping("acme/foo/tableB", id);
  HT_ASSERT(id == "1/0/2");
  mappings.push_back( Mapping("acme/foo/tableB", "1/0/2", false) );

  mapper.add_mapping("acme/bar/camp", id, NameIdMapper::IS_NAMESPACE|NameIdMapper::CREATE_INTERMEDIATE);
  HT_ASSERT(id == "1/1/0");
  mappings.push_back( Mapping("acme/bar/camp", "1/1/0", true) );
  mappings.push_back( Mapping("acme/bar", "1/1", true) );

  mapper.add_mapping("acme/bar/camp/tableA", id);
  HT_ASSERT(id == "1/1/0/0");
  mappings.push_back( Mapping("acme/bar/camp/tableA", "1/1/0/0", false) );

  mapper.add_mapping("aaa", id, NameIdMapper::IS_NAMESPACE);
  HT_ASSERT(id == "2");
  mappings.push_back( Mapping("aaa", "2", true) );

}

void test_mapper(NameIdMapper &mapper) {
  String output;
  bool is_namespace;

  for (size_t i=0; i<mappings.size(); i++) {
    HT_ASSERT(mapper.name_to_id(mappings[i].name, output, &is_namespace));
    HT_ASSERT(mappings[i].is_namespace == is_namespace);
    HT_ASSERT(output == mappings[i].id);
    HT_ASSERT(mapper.id_to_name(mappings[i].id, output, &is_namespace));
    HT_ASSERT(mappings[i].is_namespace == is_namespace);
    HT_ASSERT(output == mappings[i].name);
  }

  HT_ASSERT(!mapper.name_to_id("acme/giraffe", output));
  HT_ASSERT(!mapper.id_to_name("3/4/5", output));

  mappings.push_back( Mapping("acme/fruit", "1/2", true) );
  mapper.add_mapping("acme/fruit/rhubarb", output, NameIdMapper::CREATE_INTERMEDIATE);
  mapper.drop_mapping("acme/fruit/rhubarb");

  HT_ASSERT(!mapper.name_to_id("acme/fruit/rhubarb", output));
  HT_ASSERT(!mapper.id_to_name("1/2/0", output));

}

void cleanup(Hyperspace::SessionPtr &session, const String &toplevel_dir) {
  struct LengthDescending swo;

  sort(mappings.begin(), mappings.end(), swo);

  for (size_t i=0; i<mappings.size(); i++) {
    session->unlink(toplevel_dir + "/namemap/names/" + mappings[i].name);
    session->unlink(toplevel_dir + "/namemap/ids/" + mappings[i].id);
  }
}


} // local namesapce


int main(int argc, char *argv[]) {
  typedef Cons<DefaultServerPolicy, HyperspaceClientPolicy> MyPolicy;
  std::vector<const char *> master_args;

  if (system("/bin/rm -rf ./hsroot") != 0) {
    HT_ERROR("Problem removing ./hsroot directory");
    exit(1);
  }

  if (system("mkdir -p ./hsroot") != 0) {
    HT_ERROR("Unable to create ./hsroot directory");
    exit(1);
  }

  master_args.push_back("Hyperspace.Master");
  master_args.push_back("--config=./name_id_mapper_test.cfg");
  master_args.push_back((const char *)0);

  unlink("./Hyperspace.Master");
  HT_ASSERT(link("../../Hyperspace/Hyperspace.Master", "./Hyperspace.Master") == 0);

  {
    ServerLauncher master("./Hyperspace.Master",
                          (char * const *)&master_args[0]);

    try {
      init_with_policy<MyPolicy>(argc, argv);

      Comm *comm = Comm::instance();

      properties->set("Hyperspace.Replica.Port", (uint16_t)48122);

      SessionPtr session = new Hyperspace::Session(comm, properties);

      {
        NameIdMapper mapper(session, "/ht");

        init(mapper);
        test_mapper(mapper);
        cleanup(session, "/ht");
      }

    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      _exit(1);
    }
  }

  _exit(0);
}
