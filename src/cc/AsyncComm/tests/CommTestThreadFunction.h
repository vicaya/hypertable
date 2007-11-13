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

extern "C" {
#include <netinet/in.h>
}

#define MAX_MESSAGES 50000

/**
 *  Forward declarations
 */
namespace Hypertable {
  class Comm;
}

class CommTestThreadFunction {
 public:
  CommTestThreadFunction(Hypertable::Comm *comm, struct sockaddr_in &addr, const char *input) : m_comm(comm), m_addr(addr) {
    m_input_file = input;
  }
  void set_output_file(const char *output) { 
    m_output_file = output;
  }
  void operator()();

 private:
  Hypertable::Comm *m_comm;
  struct sockaddr_in m_addr;
  const char *m_input_file;
  const char *m_output_file;
};

