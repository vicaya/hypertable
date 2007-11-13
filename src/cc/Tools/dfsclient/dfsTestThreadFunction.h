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

#include <string>

/**
 *  Forward declarations
 */
namespace Hypertable {
  namespace DfsBroker {
    class Client;
  }
}

class dfsTestThreadFunction {
 public:
  dfsTestThreadFunction(DfsBroker::Client *client, std::string input) : m_client(client) {
    m_input_file = input;
  }
  void set_dfs_file(std::string dfsFile) { 
    m_dfs_file = dfsFile;
  }
  void set_output_file(std::string output) { 
    m_output_file = output;
  }
  void operator()();

 private:
  DfsBroker::Client *m_client;
  std::string m_input_file;
  std::string m_output_file;
  std::string m_dfs_file;
};

