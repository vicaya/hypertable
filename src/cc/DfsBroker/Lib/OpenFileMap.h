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

#include <ext/hash_map>

#include <boost/intrusive_ptr.hpp>
#include <boost/thread/mutex.hpp>

extern "C" {
#include <stdint.h>
#include <netinet/in.h>
}

#include "Common/Logger.h"
#include "Common/ReferenceCount.h"


namespace Hypertable {

  /**
   *
   */
  class OpenFileData : public ReferenceCount {
  public:
    virtual ~OpenFileData() { return; }
    struct sockaddr_in addr;    
  };
  typedef boost::intrusive_ptr<OpenFileData> OpenFileDataPtr;
  

  class OpenFileMap {

  public:

    void create(int fd, struct sockaddr_in &addr, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      dataPtr->addr = addr;
      m_file_map[fd] = dataPtr;
    }

    bool get(int fd, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      OpenFileMapT::iterator iter = m_file_map.find(fd);
      if (iter != m_file_map.end()) {
	dataPtr = (*iter).second;
	return true;
      }
      return false;
    }

    bool remove(int fd, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      OpenFileMapT::iterator iter = m_file_map.find(fd);
      if (iter != m_file_map.end()) {
	dataPtr = (*iter).second;
	m_file_map.erase(iter);
	return true;
      }
      return false;
    }

    void remove(int fd) {
      boost::mutex::scoped_lock lock(m_mutex);
      OpenFileMapT::iterator iter = m_file_map.find(fd);
      if (iter != m_file_map.end())
	m_file_map.erase(iter);
    }

    void remove_all(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(m_mutex);
      OpenFileMapT::iterator iter = m_file_map.begin();

      while (iter != m_file_map.end()) {
	if ((*iter).second->addr.sin_family == addr.sin_family &&
	    (*iter).second->addr.sin_port == addr.sin_port &&
	    (*iter).second->addr.sin_addr.s_addr == addr.sin_addr.s_addr) {
	  OpenFileMapT::iterator delIter = iter;
	  HT_ERRORF("Removing handle %d from open file map because of lost owning client connection", (*iter).first);
	  iter++;
	  m_file_map.erase(delIter);
	}
	else
	  iter++;
      }
    }

    void remove_all() {
      boost::mutex::scoped_lock lock(m_mutex);
      m_file_map.clear();
    }

  private:

    typedef __gnu_cxx::hash_map<int, OpenFileDataPtr> OpenFileMapT;

    boost::mutex  m_mutex;
    OpenFileMapT  m_file_map;
  };
}
