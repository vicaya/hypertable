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

#include <string>
#include <vector>

extern "C" {
#include <sys/poll.h>
}

#include <boost/thread/exceptions.hpp>

#include "AsyncComm/Comm.h"
#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "Hyperspace/HyperspaceClient.h"
#include "Hyperspace/HyperspaceProtocol.h"
#include "CommandCreate.h"
#include "CommandMkdirs.h"
#include "CommandAttrSet.h"
#include "CommandAttrGet.h"
#include "CommandAttrDel.h"
#include "CommandExists.h"

using namespace Hypertable;
using namespace std;


namespace {

  const char *usage[] = {
    "",
    "usage: hsTest [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<fname>  Specifies the name of the config file.",
    "  --golden          Generate hsTest.golden file",
    "",
    "This program validates the proper functioning of Hyperspace.",
    "",
    (const char *)0
  };
}



/**
 *  main
 */
int main(int argc, char **argv) {
  Properties *props = new Properties();
  Comm *comm;
  ConnectionManager *connManager;
  HyperspaceClient *client;
  int error;
  string configFile = "";
  bool golden = false;

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(2);

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strcmp(argv[i], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  if (configFile != "")
    props->load(configFile);

  comm = new Comm();

  connManager = new ConnectionManager(comm);

  client = new HyperspaceClient(connManager, props);

  if (!client->WaitForConnection(30))
    goto done;

  error = client->Exists("test");
  if (error == Error::HYPERSPACE_FILE_NOT_FOUND) {
    if ((error = client->Mkdirs("test")) != Error::OK) {
      LOG_VA_ERROR("Problem creating test directory - %s", Error::GetText(error));
      exit(1);
    }
  }
  else if (error == Error::OK) {
    client->Delete("foo");
    client->Delete("bar");
  }
  else {
    LOG_VA_ERROR("Problem testing for existance of test directory - %s", Error::GetText(error));
    exit(1);
  }

  {
    TestHarness harness("hsTest");
    DynamicBuffer dbuf(0);

    error = client->Create("foo");
    LOG_VA_INFO("CREATE 'foo' - %s", Error::GetText(error));

    error = client->Exists("foo");
    LOG_VA_INFO("EXISTS 'foo' - %s", Error::GetText(error));

    error = client->Delete("bar");
    LOG_VA_INFO("DELETE 'bar' - %s", Error::GetText(error));

    error = client->Exists("bar");
    LOG_VA_INFO("EXISTS 'bar' - %s", Error::GetText(error));

    error = client->AttrSet("foo", "name", "Doug Judd");
    LOG_VA_INFO("ATTRSET foo.name - %s", Error::GetText(error));

    if ((error = client->AttrGet("foo", "name", dbuf)) == Error::OK) {
      LOG_VA_INFO("foo.name = '%s'", (const char *)dbuf.buf);
    }
    else {
      LOG_VA_INFO("ATTRGET foo.name - %s", Error::GetText(error));
    }

    error = client->AttrSet("foo", "address", "2999 Canyon Rd. Burlingame CA 94010");
    LOG_VA_INFO("ATTRSET foo.address - %s", Error::GetText(error));

    if ((error = client->AttrGet("foo", "address", dbuf)) == Error::OK) {
      LOG_VA_INFO("foo.address = '%s'", (const char *)dbuf.buf);
    }
    else {
      LOG_VA_INFO("ATTRGET foo.address - %s", Error::GetText(error));
    }

    error = client->AttrGet("foo", "phone", dbuf);
    LOG_VA_INFO("ATTRGET foo.phone - %s", Error::GetText(error));

    error = client->AttrDel("foo", "phone");
    LOG_VA_INFO("ATTRDEL foo.phone - %s", Error::GetText(error));

    error = client->AttrDel("foo", "name");
    LOG_VA_INFO("ATTRDEL foo.name - %s", Error::GetText(error));

    error = client->Delete("foo");
    LOG_VA_INFO("DELETE foo - %s", Error::GetText(error));

    error = client->Mkdirs("chumba/wumba");
    LOG_VA_INFO("MKDIRS chumba/wumba - %s", Error::GetText(error));

    error = client->Delete("chumba");
    LOG_VA_INFO("DELETE chumba - %s", Error::GetText(error));

    error = client->Delete("chumba/wumba");
    LOG_VA_INFO("DELETE chumba/wumba - %s", Error::GetText(error));

    error = client->Delete("chumba");
    LOG_VA_INFO("DELETE chumba - %s", Error::GetText(error));

    if (!golden)
      harness.ValidateAndExit("hsTest.golden");

    harness.RegenerateGoldenFile("hsTest.golden");

  }

 done:
  delete client;
  delete comm;
  delete props;
  return 0;
}

