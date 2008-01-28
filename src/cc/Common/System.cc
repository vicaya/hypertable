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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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


string System::installDir;
string System::executableName;
boost::mutex System::ms_mutex;

void System::initialize(const char *argv0) {
  boost::mutex::scoped_lock lock(ms_mutex);
  const char *execPath = getenv("_");
  char cwd[1024];
  int offset;

  if (installDir != "")
    return;

  srand((unsigned)getpid());

  getcwd(cwd, 1024);
  strcat(cwd, "/");

  char *ptr = strrchr(argv0, '/');
  if (ptr == 0) {
    executableName = argv0;
#if 0
    std::string fname;
    char *path = getenv("PATH");
    char *last;
    execPath = 0;
    ptr = strtok_r(path, ":", &last);
    while (ptr) {
      fname = (std::string)ptr + "/" + executableName;
      if (FileUtils::exists(fname.c_str())) {
	execPath = fname.c_str();
	break;
      }
      ptr = strtok_r(0, ":", &last);      
    }
#endif
  }
  else
    executableName = ptr+1;

  offset = (execPath) ? strlen(execPath) - strlen(argv0) : -1;
  if (execPath && offset >= 0) {
    if (execPath[0] != '/') {
      installDir = cwd;
      installDir += execPath;
    }
    else
      installDir = execPath;
  }
  else {
    if (argv0[0] != '/') {
      installDir = cwd;
      installDir += argv0;
    }
    else
      installDir = argv0;
  }

  string::size_type pos = installDir.rfind("/bin/");
  if (pos == string::npos) {
    string::size_type pos = installDir.rfind("/");
    if (pos == string::npos)
      installDir = ".";
    else
      installDir.erase(pos);
    //cerr << "WARNING: " << executableName << " does not appear to have been run from an installation." << endl;
    //cerr << "WARNING: Using '" << installDir << "' as the installation directory" << endl;
  }
  else
    installDir.erase(pos);

  Logger::initialize(executableName.c_str());
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
