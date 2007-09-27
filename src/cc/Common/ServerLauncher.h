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

#ifndef HYPERTABLE_SERVERLAUNCHER_H
#define HYPERTABLE_SERVERLAUNCHER_H

extern "C" {
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
}

namespace hypertable {

  class ServerLauncher {
  public:
    ServerLauncher(const char *path, char *const argv[]) {
      int fd[2];
      if (pipe(fd) < 0) {
	perror("pipe");
	exit(1);
      }
      if ((mChildPid = fork()) == 0) {
	close(fd[1]);
	dup2(fd[0], 0);
	close(fd[0]);
	execv(path, argv);
      }
      close(fd[0]);
      mWriteFd = fd[1];
      poll(0,0,2000);
    }
    ~ServerLauncher() {
      if (kill(mChildPid, 9) == -1)
	perror("kill");
    }
    int GetWriteDescriptor() { return mWriteFd; }
    pid_t GetPid() { return mChildPid; }
    private:
      pid_t mChildPid;
      int   mWriteFd;
  };

}

#endif // HYPERTABLE_SERVERLAUNCHER_H
