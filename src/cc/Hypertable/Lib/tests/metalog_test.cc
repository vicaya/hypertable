/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Common/FileUtils.h"
#include "Common/Init.h"
#include "Common/InetAddr.h"
#include "Common/Random.h"
#include "Common/StringExt.h"
#include "Common/Serialization.h"
#include "DfsBroker/Lib/Client.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"
#include "AsyncComm/ConnectionManager.h"

#include <iostream>
#include <fstream>


#include "Hypertable/Lib/Config.h"

#include "Hypertable/Lib/MetaLogDefinition.h"
#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include "Hypertable/Lib/MetaLogWriter.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace Hypertable {
  namespace MetaLog {

    class EntityGeneric : public Entity {
    public:
      EntityGeneric(int t) : Entity(t), value(1) { m_name = String("GenericEntity") + t; }
      EntityGeneric(const EntityHeader &header_) : Entity(header_), value(0) { m_name = String("GenericEntity") + header_.type; }
      virtual const String name() { return m_name; }
      virtual size_t encoded_length() const { return 4; }
      virtual void encode(uint8_t **bufp) const { 
        Serialization::encode_i32(bufp, value);
      }
      virtual void decode(const uint8_t **bufp, size_t *remainp) { 
        value = Serialization::decode_i32(bufp, remainp);
      }
      virtual void display(ostream &os) { os << "value=" << value; }
      void increment() { value++; }
    
    private:
      String m_name;
      int32_t value;
    };

    class TestDefinition : public Definition {
    public:
      virtual uint16_t version() { return 1; }
      virtual const char *name() { return "foo"; }
      virtual Entity *create(const EntityHeader &header) {
        return new EntityGeneric(header);
      }
    };

  }
}

namespace {

  struct MyPolicy : Policy {
    static void init_options() {
      cmdline_desc().add_options()
        ("save,s", "Don't delete generated the log files")
        ;
    }
  };

  typedef Meta::list<MyPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

  MetaLog::DefinitionPtr g_test_definition = new MetaLog::TestDefinition();

  vector<MetaLog::EntityPtr> g_entities;

  void create_entities(int count) {
    for (size_t i=65536; i<=(size_t)65536+count; i++)
      g_entities.push_back( new MetaLog::EntityGeneric(i) );
  }

  void randomly_change_states(MetaLog::WriterPtr &writer) {
    MetaLog::EntityGeneric *generic_entity;
    size_t j;
    for (size_t i=0; i<256; i++) {
      j = Random::number32() % g_entities.size();
      generic_entity = (MetaLog::EntityGeneric *)g_entities[j].get();
      if (generic_entity) {
        generic_entity->increment();
        if ((i%127)==0) {
          writer->record_removal(generic_entity);
          g_entities[j] = 0;
        }
        else
          writer->record_state(generic_entity);
      }
    }
  }

  void display_entities(ofstream &out) {
    for (size_t i=0; i<g_entities.size(); i++) {
      if (g_entities[i])
        out << *g_entities[i] << "\n";
    }
    out << flush;
  }

} // local namespace


int
main(int ac, char *av[]) {
  try {
    init_with_policies<Policies>(ac, av);

    int timeout = has("dfs-timeout") ? get_i32("dfs-timeout") : 180000;
    String host = get_str("dfs-host");
    uint16_t port = get_i16("dfs-port");

    DfsBroker::Client *client = new DfsBroker::Client(host, port, timeout);

    if (!client->wait_for_connection(timeout)) {
      HT_ERROR_OUT <<"Unable to connect to DFS: "<< host <<':'<< port << HT_END;
      return 1;
    }

    FilesystemPtr fs = client;
    String testdir = format("/metalog%09d", (int)getpid());
    MetaLog::WriterPtr writer;
    MetaLog::ReaderPtr reader;

    fs->mkdirs(testdir);

    MetaLog::EntityHeader::display_timestamp = false;

    /**
     *  Write initital log
     */

    writer = new MetaLog::Writer(fs, g_test_definition, 
                                 testdir + "/" + g_test_definition->name(),
                                 g_entities);
    
    create_entities(32);

    {
      ofstream out("metalog_test.out");
      randomly_change_states(writer);
      writer = 0;
      reader = new MetaLog::Reader(fs, g_test_definition,
                                   testdir + "/" + g_test_definition->name());
      g_entities.clear();
      reader->get_entities(g_entities);
      display_entities(out);
      reader = 0;
      HT_ASSERT(FileUtils::size("metalog_test.out") == FileUtils::size("metalog_test.golden"));
    }

    /**
     *  Write some more
     */

    writer = new MetaLog::Writer(fs, g_test_definition, 
                                 testdir + "/" + g_test_definition->name(),
                                 g_entities);

    {
      ofstream out("metalog_test2.out");
      randomly_change_states(writer);
      writer = 0;
      reader = new MetaLog::Reader(fs, g_test_definition,
                                   testdir + "/" + g_test_definition->name());
      g_entities.clear();
      reader->get_entities(g_entities);
      display_entities(out);
      reader = 0;
      HT_ASSERT(FileUtils::size("metalog_test2.out") == FileUtils::size("metalog_test2.golden"));
    }

    /**
     *  Write another log and skip the RECOVER entry
     */

    MetaLog::Writer::skip_recover_entry = true;

    writer = new MetaLog::Writer(fs, g_test_definition,
                                 testdir + "/" + g_test_definition->name(),
                                 g_entities);

    {
      ofstream out("metalog_test3.out");
      randomly_change_states(writer);
      writer = 0;
      reader = new MetaLog::Reader(fs, g_test_definition,
                                   testdir + "/" + g_test_definition->name());
      g_entities.clear();
      reader->get_entities(g_entities);
      display_entities(out);
      reader = 0;
      HT_ASSERT(FileUtils::size("metalog_test3.out") == FileUtils::size("metalog_test2.golden"));
    }

    if (!has("save"))
      fs->rmdir(testdir);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
