/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    string::size_type pos = installDir.rfind("/.libs/");
    if (pos == string::npos) {
      cerr << "Unable to determine installation directory (" << installDir << ")" << endl;
      exit(1);
    }
    else
      installDir.erase(pos);
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
