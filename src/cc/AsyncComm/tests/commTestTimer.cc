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

#include <cstdlib>

extern "C" {
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/TestHarness.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

using namespace hypertable;

namespace {
  const char *usage[] = {
    "usage: commTestTimer",
    "",
    "This program tests tests timer logic of AsyncComm.",
    0
  };

  /**
   * 
   */
  class TimerHandler : public DispatchHandler {
  public:
    TimerHandler(const char *msg, ostream &out) : mMsg(msg), mOut(out) { return; }
    virtual void handle(EventPtr &eventPtr) {
      mOut << eventPtr->toString().c_str() << " (" << mMsg << ")" << endl;
    }
  private:
    const char *mMsg;
    ostream    &mOut;
  };

  struct TimerRec {
    uint32_t delay;
    const char *msg;
  };

  struct TimerRec history[] = {
    { 1, "Local" },
    { 2, "media" },
    { 3, "have" },
    { 4, "reported" },
    { 5, "eyewitness" },
    { 6, "accounts" },
    { 7, "of" },
    { 8, "a" },
    { 9, "fiery" },
    { 10, "ball" },
    { 35, 0 }
  };
}


int main(int argc, char **argv) {
  Comm *comm;
  int error;
  TestHarness harness("commTestTimer");
  bool golden = false;
  TimerHandler *timerHandler;
  int waitTime = 0;
  ostream &out = harness.GetLogStream();

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  srand(8876);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  comm = new Comm();

  for (int i=0; history[i].msg; i++) {
    timerHandler = new TimerHandler(history[i].msg, out);
    if ((error = comm->SetTimer(history[i].delay*1000, timerHandler)) != Error::OK) {
      LOG_VA_ERROR("Problem setting timer - %s", Error::GetText(error));
      exit(1);
    }
    if (history[i].delay > waitTime)
      waitTime = history[i].delay;
  }

  poll(0, 0, (waitTime+1)*1000);

  if (!golden)
    harness.ValidateAndExit("commTestTimer.golden");

  harness.RegenerateGoldenFile("commTestTimer.golden");

  delete comm;

  return 0;
}
