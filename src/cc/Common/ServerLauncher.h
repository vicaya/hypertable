/** -*- C++ -*-
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

#include <iostream>

extern "C" {
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
#include <poll.h>
}

namespace Hypertable {

  class ServerLauncher {
  public:
    ServerLauncher(const char *path, char *const argv[], const char *outputFile=0) {
      int fd[2];
      m_path = path;
      if (pipe(fd) < 0) {
	perror("pipe");
	exit(1);
      }
      if ((m_child_pid = fork()) == 0) {
	if (outputFile) {
	  int outfd = open(outputFile, O_CREAT|O_TRUNC|O_WRONLY, 0644);
	  if (outfd < 0) {
	    perror("open");
	    exit(1);
	  }
	  dup2(outfd, 1);
	}
	close(fd[1]);
	dup2(fd[0], 0);
	close(fd[0]);
	execv(path, argv);
      }
      close(fd[0]);
      m_write_fd = fd[1];
      poll(0,0,2000);
    }

    ~ServerLauncher() {
      std::cerr << "Killing '" << m_path << "' pid=" << m_child_pid << std::endl << std::flush;
      if (kill(m_child_pid, 9) == -1)
	perror("kill");
    }

    int get_write_descriptor() { return m_write_fd; }

    pid_t get_pid() { return m_child_pid; }

  private:
    const char *m_path;
    pid_t m_child_pid;
    int   m_write_fd;
  };

}

#endif // HYPERTABLE_SERVERLAUNCHER_H
