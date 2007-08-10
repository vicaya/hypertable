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


#ifndef HYPERTABLE_SCHEMA_H
#define HYPERTABLE_SCHEMA_H

#include <list>

#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include <expat.h>

#include "Common/Properties.h"

using namespace hypertable;
using namespace std;

namespace hypertable {

  class Schema {
  public:

    class ColumnFamily {
    public:
      ColumnFamily() : name(), lg(), id(0), keepCopies(0), expireTime(0) { return; }
      string   name;
      string   lg;
      uint32_t id;
      uint32_t keepCopies;
      time_t   expireTime;
    };

    class AccessGroup {
    public:
      AccessGroup() : name(), inMemory(false), columns() { return; }
      string name;
      bool   inMemory;
      list<ColumnFamily *> columns;
    };

    Schema(bool readIds=false);

    static Schema *NewInstance(const char *buf, int len, bool readIds=false);
    friend Schema *NewInstance(const char *buf, int len, bool readIds=false);

    void OpenAccessGroup();
    void CloseAccessGroup();
    void OpenColumnFamily();
    void CloseColumnFamily();
    void SetAccessGroupParameter(const char *param, const char *value);
    void SetColumnFamilyParameter(const char *param, const char *value);

    void AssignIds();

    void Render(std::string &output);

    bool IsValid();

    const char *GetErrorString() { 
      return (mErrorString.length() == 0) ? 0 : mErrorString.c_str();
    }

    void SetErrorString(std::string errStr) {
      if (mErrorString.length() == 0)
	mErrorString = errStr;
    }

    void SetGeneration(const char *generation);
    int32_t GetGeneration() { return mGeneration; }

    size_t GetMaxColumnFamilyId() { return mMaxColumnFamilyId; }

    list<AccessGroup *> *GetAccessGroupList() { return &mAccessGroups; }

    ColumnFamily *GetColumnFamily(string &name) { return mColumnFamilyMap[name]; }

  private:

    typedef __gnu_cxx::hash_map<string, ColumnFamily *> ColumnFamilyMapT;
    typedef __gnu_cxx::hash_map<string, AccessGroup *> AccessGroupMapT;

    std::string mErrorString;
    int    mNextColumnId;
    AccessGroupMapT mAccessGroupMap;
    ColumnFamilyMapT mColumnFamilyMap;
    int32_t mGeneration;
    list<AccessGroup *> mAccessGroups;
    AccessGroup *mOpenAccessGroup;
    ColumnFamily  *mOpenColumnFamily;
    bool           mReadIds;
    bool           mOutputIds;
    size_t         mMaxColumnFamilyId;

    static void StartElementHandler(void *userData, const XML_Char *name, const XML_Char **atts);
    static void EndElementHandler(void *userData, const XML_Char *name);
    static void CharacterDataHandler(void *userData, const XML_Char *s, int len);

    static Schema        *msSchema;
    static std::string    msCollectedText;
    static boost::mutex  msMutex;
  };

  typedef boost::shared_ptr<Schema> SchemaPtr;

}

#endif // HYPERTABLE_SCHEMA_H
