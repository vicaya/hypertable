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
#include "Tools/Lib/CommandShell.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"

using namespace Hypertable;
using namespace Config;

int main(int argc, char **argv) {
  typedef Cons<CommandShellPolicy, DefaultCommPolicy> MyPolicy;

  CommandShellPtr shell;
  CommandInterpreterPtr interp;
  Client *hypertable;

  try {
    init_with_policy<MyPolicy>(argc, argv);

    hypertable = new Hypertable::Client();
    interp = new HqlCommandInterpreter(hypertable);
    shell = new CommandShell("hypertable", interp, properties);
    interp->set_silent(shell->silent());
    interp->set_test_mode(shell->test_mode());

    return shell->run();
  }
  catch(Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
