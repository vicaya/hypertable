/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"

#include <boost/algorithm/string.hpp>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>

extern "C" {
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <unistd.h>
}

#include "FileUtils.h"
#include "Logger.h"
#include "System.h"

using namespace Hypertable;
using namespace std;


string System::install_dir;
string System::exe_name;
bool   System::ms_initialized = false;
Mutex        System::ms_mutex;
boost::mt19937 System::ms_rng;

String System::locate_install_dir(const char *argv0) {
  ScopedLock lock(ms_mutex);
  const char *exepath = getenv("_");
  char cwd[1024];
  int offset;

  if (install_dir != "")
    return install_dir;

  getcwd(cwd, 1024);
  strcat(cwd, "/");

  char *ptr = strrchr(argv0, '/');
  if (ptr == 0) {
    exe_name = argv0;
  }
  else
    exe_name = ptr+1;

  offset = (exepath) ? strlen(exepath) - strlen(argv0) : -1;
  if (exepath && offset >= 0) {
    if (exepath[0] != '/') {
      install_dir = cwd;
      install_dir += exepath;
    }
    else
      install_dir = exepath;
  }
  else {
    if (argv0[0] != '/') {
      install_dir = cwd;
      install_dir += argv0;
    }
    else
      install_dir = argv0;
  }

  string::size_type pos = install_dir.rfind("/bin/");
  if (pos == string::npos) {
    string::size_type pos = install_dir.rfind("/");
    if (pos == string::npos)
      install_dir = ".";
    else
      install_dir.erase(pos);
  }
  else
    install_dir.erase(pos);

  return install_dir;
}


void System::_init(const String &install_directory) {
  ScopedLock lock(ms_mutex);

  // seed the random number generator
  ms_rng.seed((uint32_t)getpid());

  // set installation directory
  install_dir = install_directory;
  while (boost::ends_with(install_dir, "/"))
    install_dir = install_dir.substr(0, install_dir.length()-1);

  if (exe_name == "")
    exe_name = "unknown";

  // initialize logging system
  Logger::initialize(exe_name);
}


int System::get_processor_count() {
#if defined(__APPLE__)
  int mib[2], ncpus;
  size_t len;

  mib[0] = CTL_HW;
  mib[1] = HW_NCPU;
  len = sizeof(ncpus);
  sysctl(mib, 2, &ncpus, &len, NULL, 0);
  return ncpus;
#elif defined(__linux__)
  return (int)sysconf(_SC_NPROCESSORS_ONLN);
#else
  ImplementMe;
#endif
}
