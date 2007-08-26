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

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

extern "C" {
#include <stdint.h>
#include <netinet/in.h>
}

namespace hypertable {

  template<typename _OpenFileData> class OpenFileMap {

  public:

    typedef boost::shared_ptr<_OpenFileData> OpenFileDataPtr;

    typedef struct {
      struct sockaddr_in addr;
      OpenFileDataPtr    dataPtr;
    } MetaValueT;

    typedef __gnu_cxx::hash_map<int, MetaValueT> OpenFileMapT;

    void Create(int fd, struct sockaddr_in &addr, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      MetaValueT meta;
      meta.addr = addr;
      meta.dataPtr = dataPtr;
      mFileMap.put(fd, meta);
    }

    bool Get(int fd, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      OpenFileMapT::iterator iter = mFileMap.find(fd);
      if (iter != mFileMap.end()) {
	dataPtr = (*iter).second.dataPtr;
	return true;
      }
      return false;
    }

    bool Remove(int fd, OpenFileDataPtr &dataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      OpenFileMapT::iterator iter = mFileMap.find(fd);
      if (iter != mFileMap.end()) {
	dataPtr = (*iter).second.dataPtr;
	mFileMap.erase(iter);
	return true;
      }
      return false;
    }

    void Remove(int fd) {
      boost::mutex::scoped_lock lock(mMutex);
      OpenFileMapT::iterator iter = mFileMap.find(fd);
      if (iter != mFileMap.end())
	mFileMap.erase(iter);
    }

    void RemoveAll(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      OpenFileMapT::iterator iter = mFileMap.begin();

      while (iter != mFileMap.end()) {
	if ((*iter).second.addr.sin_family == addr.sin_family &&
	    (*iter).second.addr.sin_port == addr.sin_port &&
	    (*iter).second.addr.sin_addr.s_addr == addr.sin_addr.s_addr) {
	  OpenFileMapT::iterator delIter = iter;
	  LOG_VA_ERROR("Removing handle %d from open file map because of lost owning client connection", (*iter).first);
	  iter++;
	  mFileMap.erase(delIter);
	}
	else
	  iter++;
      }
    }

  private:

    boost::mutex  mMutex;
    OpenFileMapT  mFileMap;
  };
}

