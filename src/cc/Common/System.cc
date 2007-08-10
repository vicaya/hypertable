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

#include <iostream>
#include <string>
using namespace std;

extern "C" {
#include <stdlib.h>
#include <sys/types.h>
#include <sys/sysctl.h>
#include <unistd.h>
}

#include "Logger.h"
#include "System.h"

using namespace hypertable;
using namespace std;


string System::installDir;
string System::executableName;


void System::Initialize(const char *argv0) {
  char *execPath = getenv("_");
  char cwd[1024];

  getcwd(cwd, 1024);
  strcat(cwd, "/");

  char *ptr = strrchr(argv0, '/');
  executableName = (ptr == 0) ? argv0 : ptr+1;

  if (execPath && (ptr = strstr(execPath, argv0)) != 0 && (ptr-execPath) == (int)(strlen(execPath)-strlen(argv0))) {
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
    cerr << "WARNING: " << executableName << " does not appear to have been run from an installation." << endl;
    cerr << "WARNING: Using '" << installDir << "' as the installation directory" << endl;
  }
  else
    installDir.erase(pos);

  Logger::Initialize(executableName.c_str());
}


int System::GetProcessorCount() {
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
