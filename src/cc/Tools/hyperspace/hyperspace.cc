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
#include <cstdio>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/thread/exceptions.hpp>

#include "Hyperspace/Config.h"
#include "Hyperspace/Session.h"
#include "Hyperspace/HsCommandInterpreter.h"
#include "Hyperspace/HsClientState.h"
#include "Common/CommandShell.h"
#include "AsyncComm/Comm.h"

using namespace Hypertable;
using namespace Config;
using namespace Hyperspace;
using namespace std;
using namespace boost;


/**
 *
 */
class SessionHandler : public SessionCallback {
public:
  virtual void jeopardy() { cout << "SESSION CALLBACK: Jeopardy" << endl << flush; }
  virtual void safe() { cout << "SESSION CALLBACK: Safe" << endl << flush; }
  virtual void expired() { cout << "SESSION CALLBACK: Expired" << endl << flush; }
};

int main(int argc, char **argv) {
  typedef Cons<HyperspaceCommandShellPolicy, DefaultCommPolicy> MyPolicy;

  CommandShellPtr shell;
  CommandInterpreterPtr interp;
  Session *session;
  Comm *comm;
  SessionHandler session_handler;

  try {
    init_with_policy<MyPolicy>(argc, argv);
    HsClientState::exit_status = 0;
    comm = Comm::instance();
    
    session = new Hyperspace::Session(comm, properties, &session_handler);
    
    interp = session->create_hs_interpreter();
    shell = new CommandShell("hyperspace", interp, properties);
    interp->set_silent(shell->silent());
    interp->set_test_mode(shell->test_mode());
    
    
    if(!session->wait_for_connection(30000)) {
      cerr << "Unable to establish connection with Hyperspace, exitting..." << endl;
      exit(1);
    }

    HsClientState::exit_status = shell->run();
    return HsClientState::exit_status;
  }
  catch(Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;

}

