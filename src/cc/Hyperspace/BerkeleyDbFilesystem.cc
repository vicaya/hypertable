/** -*- c++ -*-
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <vector>

#include <cstdlib>

#include <boost/algorithm/string.hpp>

#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/String.h"

#include "BerkeleyDbFilesystem.h"
#include "DbtManaged.h"

using namespace boost::algorithm;
using namespace Hyperspace;
using namespace Hypertable;
using namespace Error;
using namespace StateDbKeys;

#define HT_DEBUG_ATTR(_txn_, _fn_, _an_, _k_, _v_) \
  HT_DEBUG_OUT <<"txn="<< (_txn_) <<" fname='"<< (_fn_) <<"' attr='"<< (_an_) \
      <<"' key='"<< (char *)(_k_).get_data() <<"' value='"<< (_v_) <<"'" \
      << HT_END

#define HT_DEBUG_ATTR_(_txn_, _fn_, _an_, _k_, _v_, _l_) \
  HT_DEBUG_OUT <<"txn="<< (_txn_) <<" fname='"<< (_fn_); \
  if ((_an_) == String()) _out_ <<"' attr='"<< (_an_); \
  _out_ <<"' key='" << (char *)(_k_).get_data(); \
  if (_l_) _out_ <<"' value='"<< format_bytes(20, _v_, _l_); \
  _out_ <<"'"<< HT_END

/**
 */
BerkeleyDbFilesystem::BerkeleyDbFilesystem(const std::string &basedir,
                                           bool force_recover)
    : m_base_dir(basedir), m_env(0) {
  DbTxn *txn = NULL;

  u_int32_t env_flags =
    DB_CREATE |      // If the environment does not exist, create it
    DB_INIT_LOCK  |  // Initialize locking
    DB_INIT_LOG   |  // Initialize logging
    DB_INIT_MPOOL |  // Initialize the cache
    DB_INIT_TXN   |  // Initialize transactions
    DB_RECOVER    |  // Do basic recovery
    DB_THREAD;

  if (force_recover)
    env_flags |= DB_RECOVER_FATAL;

  u_int32_t db_flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD;

  /**
   * Open Berkeley DB environment and namespace database
   */
  try {
    int ret;
    Dbt key, data;
    DbtManaged keym, datam;
    char numbuf[17];

    m_env.set_lk_detect(DB_LOCK_DEFAULT);

    m_env.open(m_base_dir.c_str(), env_flags, 0);
    m_db = new Db(&m_env, 0);
    m_db->open(NULL, "namespace.db", NULL, DB_BTREE, db_flags, 0);

    // Haven't implemented state recovery yet, so delete the old statedb and create the new one
    Db old_state_db1(&m_env, 0);
    old_state_db1.open(NULL, "state.db", NULL, DB_BTREE, db_flags, 0);
    old_state_db1.close(0);
    Db old_state_db2(&m_env, 0);
    old_state_db2.remove("state.db", NULL, 0);
    //open state db
    m_state_db = new Db(&m_env, 0);
    m_state_db->set_flags(DB_DUP|DB_REVSPLITOFF);
    m_state_db->open(NULL, "state.db", NULL, DB_BTREE, db_flags, 0);

    txn = start_transaction(NULL);

    key.set_data((void *)"/");
    key.set_size(2);

    data.set_flags(DB_DBT_REALLOC);

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND) {
      data.set_data(0);
      data.set_size(0);
      ret = m_db->put(txn, &key, &data, 0);
      key.set_data((void *)"/hyperspace/");
      key.set_size(strlen("/hyperspace/")+1);
      ret = m_db->put(txn, &key, &data, 0);
      key.set_data((void *)"/hyperspace/metadata");
      key.set_size(strlen("/hyperspace/metadata")+1);
      ret = m_db->put(txn, &key, &data, 0);
    }

    if (data.get_data() != 0)
      free(data.get_data());

    // initialize statedb if reqd
    key.set_data((void *)"/");
    key.set_size(2);

    data.set_flags(DB_DBT_REALLOC);
    data.set_data(0);
    data.set_size(0);

    if((ret = m_state_db->get(txn, &key, &data, 0)) == DB_NOTFOUND) {
      data.set_data(0);
      data.set_size(0);
      ret = m_state_db->put(txn, &key, &data, 0);
      HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    }
    // init next ids in statedb
    keym.set_str(NEXT_SESSION_ID);
    if ((ret = m_state_db->get(txn, &keym, &datam, 0)) == DB_NOTFOUND) {
      sprintf(numbuf, "%llu", (Llu)1);
      datam.set_str(numbuf);
      ret = m_state_db->put(txn, &keym, &datam, 0);
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    }

    keym.set_str(NEXT_HANDLE_ID);
    if ( (ret = m_state_db->get(txn, &keym, &datam, 0)) == DB_NOTFOUND) {
      sprintf(numbuf, "%llu", (Llu)1);
      datam.set_str(numbuf);
      ret = m_state_db->put(txn, &keym, &datam, 0);
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    }

    keym.set_str(NEXT_EVENT_ID);
    if ((ret = m_state_db->get(txn, &keym, &datam, 0)) == DB_NOTFOUND) {
      sprintf(numbuf, "%llu", (Llu)1);
      datam.set_str(numbuf);
      ret = m_state_db->put(txn, &keym, &datam, 0);
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    }

    if (data.get_data() != 0)
      free(data.get_data());
    txn->commit(0);

  }
  catch(DbException &e) {
    txn->abort();
    HT_FATALF("Error initializing Berkeley DB (dir=%s) - %s",
              m_base_dir.c_str(), e.what());
  }
  HT_DEBUG_OUT <<"namespace initialized"<< HT_END;
}


/**
 */
BerkeleyDbFilesystem::~BerkeleyDbFilesystem() {
  /**
   * Close Berkeley DB "namespace" database and environment
   */
  try {
    m_db->close(0);
    m_state_db->close(0);
    delete m_db;
    delete m_state_db;
    m_env.close(0);
  }
  catch(DbException &e) {
    HT_ERRORF("Error closing Berkeley DB (dir=%s) - %s",
              m_base_dir.c_str(), e.what());
  }
  HT_DEBUG_OUT <<"namespace closed"<< HT_END;
}


DbTxn *BerkeleyDbFilesystem::start_transaction(DbTxn *parent_txn) {
  DbTxn *txn = NULL;

  // begin transaction
  try {
    m_env.txn_begin(parent_txn, &txn, 0);
  }
  catch (DbException &e) {
    HT_FATALF("Error starting Berkeley DB transaction: %s", e.what());
  }
  HT_DEBUG_OUT <<"txn="<< txn << " parent_txn=" << parent_txn << HT_END;

  return txn;
}


/**
 */
bool
BerkeleyDbFilesystem::get_xattr_i32(DbTxn *txn, const String &fname,
    const String &aname, uint32_t *valuep) {
  int ret;
  Dbt key;
  DbtManaged data;
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  try {
    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      *valuep = strtoll((const char *)data.get_data(), 0, 0);
      HT_DEBUG_ATTR(txn, fname, aname, key, *valuep);
      return true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  return false;
}



/**
 */
void
BerkeleyDbFilesystem::set_xattr_i32(DbTxn *txn, const String &fname,
                                    const String &aname, uint32_t value) {
  int ret;
  Dbt key, data;
  char numbuf[16];
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  sprintf(numbuf, "%u", value);

  data.set_data(numbuf);
  data.set_size(strlen(numbuf)+1);

  try {
    ret = m_db->put(txn, &key, &data, 0);
    HT_DEBUG_ATTR(txn, fname, aname, key, value);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  HT_EXPECT(ret == 0, HYPERSPACE_BERKELEYDB_ERROR);
}


/**
 */
bool
BerkeleyDbFilesystem::get_xattr_i64(DbTxn *txn, const String &fname,
    const String &aname, uint64_t *valuep) {
  int ret;
  Dbt key;
  DbtManaged data;
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  try {
    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      *valuep = strtoll((const char *)data.get_data(), 0, 0);
      HT_DEBUG_ATTR(txn, fname, aname, key, *valuep);
      return true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  return false;
}



/**
 */
void
BerkeleyDbFilesystem::set_xattr_i64(DbTxn *txn, const String &fname,
                                    const String &aname, uint64_t value) {
  int ret;
  Dbt key, data;
  char numbuf[24];
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  sprintf(numbuf, "%llu", (Llu)value);

  data.set_data(numbuf);
  data.set_size(strlen(numbuf)+1);

  try {
    ret = m_db->put(txn, &key, &data, 0);
    HT_DEBUG_ATTR(txn, fname, aname, key, value);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  HT_EXPECT(ret == 0, HYPERSPACE_BERKELEYDB_ERROR);
}


/**
 */
void
BerkeleyDbFilesystem::set_xattr(DbTxn *txn, const String &fname,
    const String &aname, const void *value, size_t value_len) {
  int ret;
  Dbt key, data;
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);
  data.set_data((void *)value);
  data.set_size(value_len);

  try {
    HT_DEBUG_ATTR_(txn, fname, aname, key, value, value_len);
    ret = m_db->put(txn, &key, &data, 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  HT_EXPECT(ret == 0, HYPERSPACE_BERKELEYDB_ERROR);
}


/**
 */
bool
BerkeleyDbFilesystem::get_xattr(DbTxn *txn, const String &fname,
                                const String &aname, DynamicBuffer &vbuf) {
  int ret;
  Dbt key;
  DbtManaged data;
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  HT_DEBUG_OUT << "get_xattr txn="<< txn <<", fname=" << fname << ", attr='" << aname
               << "', key=" << key.get_str() << HT_END;

  try {
    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      vbuf.reserve(data.get_size());
      memcpy(vbuf.base, (uint8_t *)data.get_data(), data.get_size());
      vbuf.ptr += data.get_size();
      HT_DEBUG_ATTR_(txn, fname, aname, key, vbuf.base, data.get_size());
      return true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  return false;
}

/**
 */
bool
BerkeleyDbFilesystem::exists_xattr(DbTxn *txn, const String &fname, const String &aname)
{
  int ret;
  Dbt key;
  String keystr = fname;
  bool exists = false;

  build_attr_key(txn, keystr, aname, key);

  try {
    ret = m_db->exists(txn, &key, 0);
    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_BERKELEYDB_ERROR);

    if (ret == 0) {
      exists = true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  return exists;
}


void
BerkeleyDbFilesystem::del_xattr(DbTxn *txn, const String &fname,
                                const String &aname) {
  int ret;
  Dbt key;
  String keystr = fname;

  build_attr_key(txn, keystr, aname, key);

  try {
    if ((ret = m_db->del(txn, &key, 0)) == DB_NOTFOUND)
      HT_THROW(HYPERSPACE_ATTR_NOT_FOUND, aname);
    HT_DEBUG_ATTR_(txn, fname, aname, key, "", 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }
}


/**
 */
void BerkeleyDbFilesystem::mkdir(DbTxn *txn, const String &name) {
  int ret;
  Dbt key;
  DbtManaged data;
  size_t lastslash = name.rfind('/', name.length()-1);
  String dirname = name.substr(0, lastslash+1);

  try {
    /**
     * Make sure parent directory exists
     */
    key.set_data((void *)dirname.c_str());
    key.set_size(dirname.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND)
      HT_THROW(HYPERSPACE_FILE_NOT_FOUND, dirname);

    // formulate directory name
    dirname = name;
    if (dirname[dirname.length()-1] != '/')
      dirname += "/";

    HT_DEBUG_OUT <<"dirname='"<< dirname <<"'"<< HT_END;

    /**
     * Make sure directory does not already exists
     */
    key.set_data((void *)dirname.c_str());
    key.set_size(dirname.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) != DB_NOTFOUND)
      HT_THROW(HYPERSPACE_FILE_EXISTS, dirname);

    /**
     * Create directory entry
     */
    key.set_data((void *)dirname.c_str());
    key.set_size(dirname.length()+1);

    data.clear();

    ret = m_db->put(txn, &key, &data, 0);

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }
}


void BerkeleyDbFilesystem::unlink(DbTxn *txn, const String &name) {
  std::vector<String> delkeys;
  DbtManaged keym, datam;
  Dbt key;
  Dbc *cursorp = 0;
  bool looks_like_dir = false;
  bool looks_like_file = false;
  String str;

  try {
    m_db->cursor(txn, &cursorp, 0);

    keym.set_str(name);

    if (cursorp->get(&keym, &datam, DB_SET_RANGE) != DB_NOTFOUND) {
      do {
        str = keym.get_str();

        if (str.length() > name.length()) {
          if (str[name.length()] == '/') {
            if (str.length() > name.length() + 1
                && str[name.length() + 1] != NODE_ATTR_DELIM) {
              cursorp->close();
              HT_THROW(HYPERSPACE_DIR_NOT_EMPTY, name);
            }
            looks_like_dir = true;
            delkeys.push_back(keym.get_str());
          }
          else if (str[name.length()] == NODE_ATTR_DELIM) {
            looks_like_file = true;
            delkeys.push_back(keym.get_str());
          }
        }
        else {
          delkeys.push_back(keym.get_str());
          looks_like_file = true;
        }

      } while (cursorp->get(&keym, &datam, DB_NEXT) != DB_NOTFOUND &&
               starts_with(keym.get_str(), name.c_str()));
    }

    cursorp->close();
    cursorp = 0;

    HT_ASSERT(!(looks_like_dir && looks_like_file));

    if (delkeys.empty())
      HT_THROW(HYPERSPACE_FILE_NOT_FOUND, name);

    for (size_t i=0; i<delkeys.size(); i++) {
      key.set_data((void *)delkeys[i].c_str());
      key.set_size(delkeys[i].length()+1);
      HT_ASSERT(m_db->del(txn, &key, 0) != DB_NOTFOUND);
      HT_DEBUG_ATTR_(txn, name, "", key, "", 0);
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }
}


bool
BerkeleyDbFilesystem::exists(DbTxn *txn, String fname, bool *is_dir_p) {
  int ret;
  Dbt key;

  if (is_dir_p)
    *is_dir_p = false;

  key.set_data((void *)fname.c_str());
  key.set_size(fname.length()+1);

  try {
    if ((ret = m_db->exists(txn, &key, 0)) == DB_NOTFOUND) {
      fname += "/";
      key.set_data((void *)fname.c_str());
      key.set_size(fname.length()+1);

      if ((ret = m_db->exists(txn, &key, 0)) == DB_NOTFOUND) {
        HT_DEBUG_OUT <<"'"<< fname <<"' does NOT exist."<< HT_END;
        return false;
      }
      if (is_dir_p)
        *is_dir_p = true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
     if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"'"<< fname <<"' exists."<< HT_END;
  return true;
}


/**
 *
 */
void
BerkeleyDbFilesystem::create(DbTxn *txn, const String &fname, bool temp) {
  int ret;
  Dbt key;
  DbtManaged data;

  HT_DEBUG_OUT <<"txn="<< txn <<" fname='"<< fname <<"' temp="<< temp << HT_END;

  try {
    if (exists(txn, fname, 0))
      HT_THROW(HYPERSPACE_FILE_EXISTS, fname);

    if (ends_with(fname, "/"))
      HT_THROW(HYPERSPACE_IS_DIRECTORY, fname);

    size_t lastslash = fname.rfind('/', fname.length() - 1);
    String parent_dir = fname.substr(0, lastslash + 1);

    key.set_data((void *)parent_dir.c_str());
    key.set_size(parent_dir.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND)
      HT_THROW(HYPERSPACE_BAD_PATHNAME, fname);

    key.set_data((void *)fname.c_str());
    key.set_size(fname.length()+1);

    ret = m_db->put(txn, &key, &data, 0);

    if (temp) {
      String temp_key = fname + NODE_ATTR_DELIM +"temp";
      key.set_data((void *)temp_key.c_str());
      key.set_size(temp_key.length()+1);
      ret = m_db->put(txn, &key, &data, 0);
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
     if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
   }
}


void
BerkeleyDbFilesystem::get_directory_listing(DbTxn *txn, String fname,
                                            std::vector<DirEntry> &listing) {
  DbtManaged keym, datam;
  Dbt key;
  Dbc *cursorp = 0;
  String str, last_str;
  DirEntry entry;
  size_t offset;

  try {
    m_db->cursor(txn, &cursorp, 0);

    if (!ends_with(fname, "/"))
      fname += "/";

    HT_DEBUG_OUT <<"txn="<< txn <<" dir='"<< fname <<"'"<< HT_END;
    keym.set_str(fname);

    if (cursorp->get(&keym, &datam, DB_SET_RANGE) != DB_NOTFOUND) {

      if (!starts_with(keym.get_str(), fname.c_str())) {
        cursorp->close();
        HT_THROW(HYPERSPACE_BAD_PATHNAME, fname);
      }

      do {
        str = keym.get_str();

        if (str.length() > fname.length()) {
          if (str[fname.length()] != NODE_ATTR_DELIM ) {
            str = str.substr(fname.length());

            if ((offset = str.find('/')) != String::npos) {
              entry.name = str.substr(0, offset);

              if (entry.name != last_str) {
                entry.is_dir = true;
                listing.push_back(entry);
                last_str = entry.name;
              }
            }
            else {
              if ((offset = str.find(NODE_ATTR_DELIM)) != String::npos) {
                entry.name = str.substr(0, offset);

                if (entry.name != last_str) {
                  entry.is_dir = false;
                  listing.push_back(entry);
                  last_str = entry.name;
                }
              }
              else {
                entry.name = str;

                if (entry.name != last_str) {
                  entry.is_dir = false;
                  listing.push_back(entry);
                  last_str = entry.name;
                }
              }
            }
          }
        }
      } while (cursorp->get(&keym, &datam, DB_NEXT) != DB_NOTFOUND &&
               starts_with(keym.get_str(), fname.c_str()));

    }

    cursorp->close();
    cursorp = 0;

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_BERKELEYDB_ERROR, e.what());
   }
}


void
BerkeleyDbFilesystem::get_all_names(DbTxn *txn,
                                    std::vector<String> &names) {
  DbtManaged keym, datam;
  Dbc *cursorp = NULL;
  int ret;

  HT_DEBUG_OUT <<"txn="<< txn << HT_END;

  try {
    m_db->cursor(txn, &cursorp, 0);

    while ((ret = cursorp->get(&keym, &datam, DB_NEXT)) == 0) {
      names.push_back(keym.get_str());
    }
  }
  catch(DbException &e) {
    HT_FATALF("Berkeley DB error: %s", e.what());
  }
  catch(std::exception &e) {
    HT_FATALF("Berkeley DB error: %s", e.what());
  }

  if (cursorp != NULL)
    cursorp->close();
}

void
BerkeleyDbFilesystem::build_attr_key(DbTxn *txn, String &keystr,
                                     const String &aname, Dbt &key) {
  bool isdir;

  if (!exists(txn, keystr, &isdir))
    HT_THROW(HYPERSPACE_FILE_NOT_FOUND, keystr);

  if (isdir)
    keystr += "/";

  keystr += NODE_ATTR_DELIM;
  keystr += aname;

  key.set_data((void *)keystr.c_str());
  key.set_size(keystr.length() + 1);
}

bool
BerkeleyDbFilesystem::list_xattr(DbTxn *txn, const String& fname,
                                 std::vector<String> &anames)
{
  DbtManaged keym, datam;
  Dbc *cursorp = NULL;
  int ret;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    HT_THROW(HYPERSPACE_FILE_NOT_FOUND, fname);

  const String targetkey(fname + (isdir ? "/" : "") + NODE_ATTR_DELIM);

  HT_DEBUG_OUT << "txn=" << txn << HT_END;
  try {
    m_db->cursor(txn, &cursorp, 0);
    while ((ret = cursorp->get(&keym, &datam, DB_NEXT)) == 0) {
      /* build a prefix of a key to match against all keys later */
      String currkey = keym.get_str();

      /* this whole key is shorter than our prefix - skip it */
      if (currkey.size() < targetkey.size())
        continue;

      size_t keysize = targetkey.size();
      if (!targetkey.compare(0, keysize, currkey, 0, keysize)) {
        /* strip everything before and including attribute
           delimiter to get the attribute name */
        String attribute(currkey.substr(keysize, currkey.size()));
        anames.push_back(attribute);
      }
    }
    if (cursorp != NULL)
      cursorp->close();
  } catch(DbException &e) {
    if (cursorp != NULL)
      cursorp->close();
    HT_FATALF("Berkeley DB error: %s", e.what());
    return false;
  } catch(std::exception &e) {
    if (cursorp != NULL)
      cursorp->close();
    HT_FATALF("Berkeley DB error: %s", e.what());
    return false;
  }

  return true;
}

/**
 *
 */
void
BerkeleyDbFilesystem::create_event(DbTxn *txn, uint32_t type, uint64_t id,
    uint32_t mask)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"create_event txn="<< txn <<" event type='"<< type <<"' id="
      << id << " mask=" << mask << HT_END;
  try {

    HT_EXPECT(!(event_exists(txn, id)), HYPERSPACE_STATEDB_EVENT_EXISTS);

    // Store id under "/EVENTS/"
    String events_dir = EVENTS_STR;
    keym.set_str(events_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    cursorp->close();
    cursorp = 0;
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store event type
    key_str = get_event_key(id, EVENT_TYPE);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)type);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    // Store event mask
    key_str = get_event_key(id, EVENT_MASK);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)mask);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }
}

/**
 *
 */
void
BerkeleyDbFilesystem::create_event(DbTxn *txn, uint32_t type, uint64_t id,
    uint32_t mask, const String &name)
{
  int ret;
  String key_str;
  DbtManaged keym, datam;
  Dbc *cursorp = 0;

  try {
    create_event(txn, type, id, mask);
    // Store event name
    key_str = get_event_key(id, EVENT_NAME);
    keym.set_str(key_str);
    datam.set_str(name);
    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }
}
/**
 *
 */
void
BerkeleyDbFilesystem::create_event(DbTxn *txn, uint32_t type, uint64_t id,
    uint32_t mask, uint32_t mode)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  try {
    create_event(txn, type, id, mask);
    // Store mode
    key_str = get_event_key(id, EVENT_MODE);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)mode);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }
}
/**
 *
 */
void
BerkeleyDbFilesystem::create_event(DbTxn *txn, uint32_t type, uint64_t id,
    uint32_t mask, uint32_t mode,
    uint64_t generation)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  try {
    create_event(txn, type, id, mask, mode);
    // Store generation
    key_str = get_event_key(id, EVENT_GENERATION);
    keym.set_str(key_str);
    sprintf(numbuf, "%llu", (Llu)generation);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_event_notification_handles(DbTxn *txn, uint64_t id,
    const vector<uint64_t> &handles)
{
  int ret;
  DbtManaged  keym;
  String key_str;
  // 4 bytes for #handles, 8 bytes/handle
  uint32_t serialized_len = handles.size() * (sizeof(uint64_t)) + sizeof(uint32_t);
  DynamicBuffer buf(serialized_len);

  HT_DEBUG_OUT <<"set_event_notification_handles txn="<< txn <<" event id="<< id
      << " num notification handles=" << handles.size() << HT_END;

  // Serialize handles vector
  Serialization::encode_i32(&buf.ptr, handles.size());
  for(uint32_t ii=0; ii< handles.size(); ++ii)
    Serialization::encode_i64(&buf.ptr, handles[ii]);

  Dbt data((void *)buf.base, serialized_len);

  try {
    HT_EXPECT(event_exists(txn, id), HYPERSPACE_STATEDB_EVENT_NOT_EXISTS);

    // Write event notification handles
    key_str = get_event_key(id, EVENT_NOTIFICATION_HANDLES);
    keym.set_str(key_str);
    // Write only if this key doesnt exist in the db
    ret = m_state_db->put(txn, &keym, &data, DB_NOOVERWRITE);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

  }
  catch (DbException &e) {
     HT_ERRORF("Berkeley DB error: %s", e.what());
     if (e.get_errno() == DB_LOCK_DEADLOCK)
       HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
     else
       HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_event(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"delete_event txn="<< txn <<" event id="<< id << HT_END;
  try {
    HT_EXPECT(event_exists(txn, id), HYPERSPACE_STATEDB_EVENT_NOT_EXISTS);

    // Delete id under "/EVENTS/"
    String events_dir = EVENTS_STR;
    keym.set_str(events_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    cursorp->get(&keym, &datam, DB_GET_BOTH);
    cursorp->del(0);
    cursorp->close();
    cursorp = 0;

    // Delete event type
    key_str = get_event_key(id, EVENT_TYPE);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret != DB_NOTFOUND, HYPERSPACE_STATEDB_EVENT_ATTR_NOT_FOUND);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete event mask
    key_str = get_event_key(id, EVENT_MASK);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete event name
    key_str = get_event_key(id, EVENT_NAME);
    keym.set_str(key_str);

    if ((ret = m_state_db->del(txn, &keym, 0)) == DB_NOTFOUND)
      HT_DEBUG_OUT <<"txn="<< txn <<" event key " << keym.get_str()
                   << "not found in DB"  << HT_END;

    // Delete event mode
    key_str = get_event_key(id, EVENT_MODE);
    keym.set_str(key_str);

    if ((ret = m_state_db->del(txn, &keym, 0)) == DB_NOTFOUND)
      HT_DEBUG_OUT <<"txn="<< txn <<" event key " << keym.get_str()
                   << " not found in DB"  << HT_END;

    // Delete event generation
    key_str = get_event_key(id, EVENT_GENERATION);
    keym.set_str(key_str);

    if ((ret = m_state_db->del(txn, &keym, 0)) == DB_NOTFOUND)
      HT_DEBUG_OUT <<"txn="<< txn <<" event key " << keym.get_str()
                   << " not found in DB"  << HT_END;

    // Delete event notification handles
    key_str = get_event_key(id, EVENT_NOTIFICATION_HANDLES);
    keym.set_str(key_str);

    if ((ret = m_state_db->del(txn, &keym, 0)) == DB_NOTFOUND)
      HT_DEBUG_OUT <<"txn="<< txn <<" event key " << keym.get_str()
                   << " not found in DB"  << HT_END;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }
}

/**
 *
 */
bool
BerkeleyDbFilesystem::event_exists(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  Dbc *cursorp = 0;
  bool exists = true;
  char numbuf[16];

  HT_DEBUG_OUT <<"event_exists txn="<< txn << "event id=" << id << HT_END;

  try {
    m_state_db->cursor(txn, &cursorp, 0);

    // Check for id under "/EVENTS/"
    String events_dir = EVENTS_STR;
    keym.set_str(events_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    if(cursorp->get(&keym, &datam, DB_GET_BOTH) == DB_NOTFOUND) {
      exists = false;
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }

  if(exists)
    HT_DEBUG_OUT <<"event id '"<< id <<"' exists."<< HT_END;

  return exists;
}

/**
 *
 */
void
BerkeleyDbFilesystem::create_session(DbTxn *txn, uint64_t id, const String& addr)

{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  String expbuf;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"create_session txn="<< txn <<" create session addr='"<< addr
              << " id="<< id << HT_END;
  try {

    HT_EXPECT(!session_exists(txn, id), HYPERSPACE_STATEDB_SESSION_EXISTS);
    // Store id under "/SESSIONS/"
    String sessions_dir = SESSIONS_STR;
    keym.set_str(sessions_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    cursorp->close();
    cursorp = 0;
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store session addr
    key_str = get_session_key(id, SESSION_ADDR);
    keym.set_str(key_str);
    datam.set_str(addr);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store session expired status
    key_str = get_session_key(id, SESSION_EXPIRED);
    keym.set_str(key_str);
    datam.set_str((String)"0");

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting create_session txn="<< txn
              <<" create session addr='"<< addr << " id="<< id << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_session(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"delete_session txn="<< txn <<" session id="<< id << HT_END;
  try {

    // Delete session expired status
    key_str = get_session_key(id, SESSION_EXPIRED);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    // Delete session handles
    String session_handles_dir = get_session_key(id, SESSION_HANDLES);
    keym.set_str(session_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);

    ret = cursorp->get(&keym, &datam, DB_SET);
    while(ret != DB_NOTFOUND) {
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
      cursorp->del(0);
      ret = cursorp->get(&keym, &datam, DB_NEXT_DUP);
    }

    // Delete session name is exists
    key_str = get_session_key(id, SESSION_NAME);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    if (ret != DB_NOTFOUND) {
      ret = cursorp->del(0);
    }

    // Delete session addr
    key_str = get_session_key(id, SESSION_ADDR);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    // Delete id under "/SESSIONS/"
    String sessions_dir = SESSIONS_STR;
    keym.set_str(sessions_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    cursorp->get(&keym, &datam, DB_GET_BOTH);
    ret = cursorp->del(0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT << "exitting delete_session txn=" << txn << " session id=" << id << HT_END;
}

/**
 *
 */
void
BerkeleyDbFilesystem::expire_session(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"expire_session txn="<< txn <<" session id="<< id << HT_END;

  try {
    m_state_db->cursor(txn, &cursorp, 0);
    // Set session expired status
    key_str = get_session_key(id, SESSION_EXPIRED);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_SESSION_ATTR_NOT_FOUND);
    datam.set_str((String)"1");

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting expire_session txn="<< txn <<" session id=" << id << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::add_session_handle(DbTxn *txn, uint64_t id, uint64_t handle_id)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  char numbuf[17];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT << "add_session_handle txn="<< txn <<" session id="<< id
               << " handle id=" << handle_id << HT_END;
  try {
    HT_EXPECT(session_exists(txn, id), HYPERSPACE_STATEDB_SESSION_NOT_EXISTS);

    String session_handles_dir = get_session_key(id, SESSION_HANDLES);
    keym.set_str(session_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting add_session_handle txn="<< txn <<" session id="<< id
              << " handle id=" << handle_id << HT_END;
}

/**
 *
 */
void
BerkeleyDbFilesystem::get_session_handles(DbTxn *txn, uint64_t id, vector<uint64_t> &handles)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"get_session_handles txn="<< txn <<" session id="<< id << HT_END;

  try {
    HT_EXPECT(session_exists(txn, id), HYPERSPACE_STATEDB_SESSION_NOT_EXISTS);

    String session_handles_dir = get_session_key(id, SESSION_HANDLES);
    keym.set_str(session_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);

    ret = cursorp->get(&keym, &datam, DB_SET);
    while(ret != DB_NOTFOUND) {
      handles.push_back(strtoul((const char *)datam.get_data(), 0, 0));
      ret = cursorp->get(&keym, &datam, DB_NEXT_DUP);
    }
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_session_handles txn="<< txn <<" session id="<< id << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::delete_session_handle(DbTxn *txn, uint64_t id, uint64_t handle_id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp;
  char numbuf[17];
  bool deleted = false;

  HT_DEBUG_OUT <<"delete_session_handle txn="<< txn <<" session id="<< id
               << " handle_id=" << handle_id << HT_END;
  try {
    HT_EXPECT(session_exists(txn, id), HYPERSPACE_STATEDB_SESSION_NOT_EXISTS);

    // Delete session handle
    String session_handles_dir = get_session_key(id, SESSION_HANDLES);
    keym.set_str(session_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);

    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);

    if (ret != DB_NOTFOUND) {
      ret = cursorp->del(0);
      HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
      deleted = true;
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting delete_session_handle txn="<< txn <<" session id="<< id
               << " handle_id=" << handle_id << " deleted=" << deleted << HT_END;
  return deleted;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::session_exists(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  Dbc *cursorp = 0;
  bool exists = true;
  char numbuf[16];

  HT_DEBUG_OUT <<"session_exists txn="<< txn << " session id=" << id << HT_END;

  try {
    m_state_db->cursor(txn, &cursorp, 0);

    // Check for id under "/SESSIONS/"
    String sessions_dir = SESSIONS_STR;
    keym.set_str(sessions_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    if(cursorp->get(&keym, &datam, DB_GET_BOTH) == DB_NOTFOUND) {
      exists = false;
    }
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }

  if(exists)
    HT_DEBUG_OUT <<"session id '"<< id <<"' exists."<< HT_END;

  HT_DEBUG_OUT <<"exitting session_exists txn="<< txn << " session id=" << id << HT_END;
  return exists;
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_session_name(DbTxn *txn, uint64_t id, const String &name)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"set_session_name txn="<< txn <<" name='"<< name << "' id="<< id << HT_END;
  try {

    m_state_db->cursor(txn, &cursorp, 0);
    // Store session name/replace if exists
    key_str = get_session_key(id, SESSION_NAME);
    keym.set_str(key_str);
    ret = cursorp->get(&keym, &datam, DB_SET);
    datam.set_str(name);
    if (ret == DB_NOTFOUND)
      ret = m_state_db->put(txn, &keym, &datam, 0);
    else
      ret = cursorp->put(&keym, &datam, DB_CURRENT);

    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_session_name txn="<< txn <<" name='"<< name << "'" << HT_END;
}

/**
 *
 */
String
BerkeleyDbFilesystem::get_session_name(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str, name;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"get_session_name txn="<< txn <<" session id="<< id << HT_END;

  try {
    HT_EXPECT(session_exists(txn, id), HYPERSPACE_STATEDB_SESSION_NOT_EXISTS);

    keym.set_str(get_session_key(id, SESSION_NAME));
    m_state_db->cursor(txn, &cursorp, 0);

    ret = cursorp->get(&keym, &datam, DB_SET);

    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);

    if (ret == DB_NOTFOUND)
      name = "";
    else
      name = datam.get_str();
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_session_name txn="<< txn
               <<" session id=" << id << " name="<< name << HT_END;
  return name;
}

/**
 *
 */
void
BerkeleyDbFilesystem::create_handle(DbTxn *txn, uint64_t id, String node_name,
    uint32_t open_flags, uint32_t event_mask, uint64_t session_id,
    bool locked)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[17];
  String buf;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"create_handle txn="<< txn <<" id="<< id << " node='"<< node_name
               << "' open_flags=" << open_flags << " event_mask="<< event_mask
               << " session_id=" << session_id << " locked=" << locked << HT_END;
  try {

    HT_EXPECT(!handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_EXISTS);
    // Store id under "/HANDLES/"
    String handles_dir = HANDLES_STR;
    keym.set_str(handles_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    cursorp->close();
    cursorp = 0;
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store handle node name
    key_str = get_handle_key(id, HANDLE_NODE_NAME);
    keym.set_str(key_str);
    datam.set_str(node_name);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store handle open_flags
    key_str = get_handle_key(id, HANDLE_OPEN_FLAGS);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)open_flags);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store handle notification event_mask
    key_str = get_handle_key(id, HANDLE_EVENT_MASK);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)event_mask);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store handle session id
    key_str = get_handle_key(id, HANDLE_SESSION_ID);
    keym.set_str(key_str);
    sprintf(numbuf, "%llu", (Llu)session_id);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store handle locked bool
    key_str = get_handle_key(id, HANDLE_LOCKED);
    keym.set_str(key_str);
    if(locked)
      buf = "1";
    else
      buf = "0";
    datam.set_str(buf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting create_handle txn="<< txn <<" id="<< id << " node='"<< node_name
               << "' open_flags=" << open_flags << " event_mask="<< event_mask
               << " session_id=" << session_id << " locked=" << locked << HT_END;
}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_handle(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"delete_handle txn="<< txn <<" handle id="<< id << HT_END;
  try {

    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    // Delete handle locked bool
    key_str = get_handle_key(id, HANDLE_LOCKED);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete handle session id
    key_str = get_handle_key(id, HANDLE_SESSION_ID);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete handle notification event_mask
    key_str = get_handle_key(id, HANDLE_EVENT_MASK);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete handle open_flags
    key_str = get_handle_key(id, HANDLE_OPEN_FLAGS);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete handle node name
    key_str = get_handle_key(id, HANDLE_NODE_NAME);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete id under "/HANDLES/"
    String handles_dir = HANDLES_STR;
    keym.set_str(handles_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    ret = cursorp->del(0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT << "exitting delete_handle txn="<< txn <<" handle id="
      << id << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::set_handle_open_flags(DbTxn *txn, uint64_t id, uint32_t open_flags)
{
  DbtManaged keym, datam;
  char numbuf[17];
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_handle_open_flags txn="<< txn <<" handle id="
               << id << " open_flags=" << open_flags << HT_END;
  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing open flag
    key_str = get_handle_key(id, HANDLE_OPEN_FLAGS);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);

    sprintf(numbuf, "%lu", (Lu)open_flags);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_handle_open_flags txn="<< txn <<" handle id="
               << id << " open_flags=" << open_flags << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::set_handle_event_mask(DbTxn *txn, uint64_t id, uint32_t event_mask)
{
  DbtManaged keym, datam;
  char numbuf[17];
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_handle_event_mask txn="<< txn <<" handle id="
               << id << " event_mask=" << event_mask << HT_END;
  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing event_mask
    key_str = get_handle_key(id, HANDLE_EVENT_MASK);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);

    sprintf(numbuf, "%lu", (Lu)event_mask);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_handle_event_mask txn="<< txn <<" handle id="
               << id << " event_mask=" << event_mask << HT_END;

}

/**
 *
 */
uint32_t
BerkeleyDbFilesystem::get_handle_event_mask(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  int ret;
  Dbc *cursorp = 0;
  String key_str;
  uint32_t event_mask;

  HT_DEBUG_OUT <<"get_handle_event_mask txn="<< txn <<" handle id=" << id << HT_END;

  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing event_mask
    key_str = get_handle_key(id, HANDLE_EVENT_MASK);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);
    event_mask = (uint32_t)strtoull(datam.get_str(), 0, 0);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_handle_event_mask txn="<< txn <<" handle id=" << id
               << " event_mask=" << event_mask << HT_END;

  return event_mask;
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_handle_node(DbTxn *txn, uint64_t id, const String &node_name)
{
  DbtManaged keym, datam;
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_handle_node_name txn="<< txn <<" handle id=" << id
               << " node_name=" << node_name << HT_END;
  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing node_name
    key_str = get_handle_key(id, HANDLE_NODE_NAME);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);
    datam.set_str(node_name);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_handle_node_name txn="<< txn <<" handle id="
               << id << " node_name=" << node_name << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::set_handle_locked(DbTxn *txn, uint64_t id, bool locked)
{
  DbtManaged keym, datam;
  int ret;
  String buf;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_handle_locked txn="<< txn <<" handle id=" << id
               << " locked=" << locked << HT_END;
  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing lockedness
    key_str = get_handle_key(id, HANDLE_LOCKED);
    keym.set_str(key_str);
    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);
    if(locked)
      buf = "1";
    else
      buf = "0";
    datam.set_str(buf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
 HT_DEBUG_OUT <<"exitting set_handle_locked txn="<< txn <<" handle id=" << id
              << " locked=" << locked << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::handle_exists(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  Dbc *cursorp = 0;
  bool exists = true;
  char numbuf[17];

  HT_DEBUG_OUT <<"handle_exists txn="<< txn << " handle id=" << id << HT_END;

  try {
    m_state_db->cursor(txn, &cursorp, 0);

    // Check for id under "/HANDLES/"
    String handles_dir = HANDLES_STR;
    keym.set_str(handles_dir);
    sprintf(numbuf, "%llu", (Llu)id);
    datam.set_str(numbuf);

    if(cursorp->get(&keym, &datam, DB_GET_BOTH) == DB_NOTFOUND) {
      exists = false;
    }
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }

  HT_DEBUG_OUT << "exitting handle_exists txn="<< txn << " handle id=" << id
               << " exists=" << exists << HT_END;
  return exists;
}

/**
 *
 */

bool
BerkeleyDbFilesystem::handle_is_locked(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  int ret;
  String buf;
  bool locked;
  String key_str;

  HT_DEBUG_OUT << "handle_is_locked txn=" << txn << " handle id=" << id << HT_END;

  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);

    // Get locked-ness
    key_str = get_handle_key(id, HANDLE_LOCKED);
    keym.set_str(key_str);
    ret = m_state_db->get(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);

    buf = datam.get_str();

    if (buf == "1")
      locked = true;
    else if (buf == "0")
      locked = false;
    else {
      HT_EXPECT(false, HYPERSPACE_STATEDB_BAD_VALUE);
    }

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_handle_locked txn="<< txn <<" handle id=" << id
               << " locked=" << locked << HT_END;
  return locked;
}

/**
 *
 */
void
BerkeleyDbFilesystem::get_handle_node(DbTxn *txn, uint64_t id, String &node_name)
{
  DbtManaged keym, datam;
  int ret;
  String key_str;

  HT_DEBUG_OUT <<"get_handle_node_name txn="<< txn <<" handle id=" << id << HT_END;

  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);

    // Get existing node_name
    key_str = get_handle_key(id, HANDLE_NODE_NAME);
    keym.set_str(key_str);

    ret = m_state_db->get(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);
    node_name = datam.get_str();

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_handle_node_name txn="<< txn <<" handle id=" << id
               <<" node_name=" << node_name << HT_END;
}

/**
 *
 */
uint32_t
BerkeleyDbFilesystem::get_handle_open_flags(DbTxn *txn, uint64_t id)
{
  DbtManaged keym, datam;
  int ret;
  String key_str;
  uint32_t open_flags;

  HT_DEBUG_OUT <<"get_handle_open_flags txn="<< txn <<" handle id=" << id << HT_END;

  try {
    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);

    // Get existing open flag
    key_str = get_handle_key(id, HANDLE_OPEN_FLAGS);
    keym.set_str(key_str);

    ret = m_state_db->get(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);

    open_flags = (uint32_t)strtoul(datam.get_str(), 0, 0);

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_handle_open_flags txn="<< txn <<" handle id="
               << id << " open_flags=" << open_flags << HT_END;
  return open_flags;
}

/**
 *
 */
uint64_t
BerkeleyDbFilesystem::get_handle_session(DbTxn *txn, uint64_t id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  uint64_t session_id;

  HT_DEBUG_OUT <<"get_handle_session txn="<< txn <<" id="<< id << HT_END;

  try {

    HT_EXPECT(handle_exists(txn, id), HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS);

    // Get handle session id
    key_str = get_handle_key(id, HANDLE_SESSION_ID);
    keym.set_str(key_str);

    ret = m_state_db->get(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND);

    session_id = (uint64_t)strtoull(datam.get_str(), 0, 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"get_handle_session txn="<< txn <<" id="<< id << " session_id="
               << session_id << HT_END;

  return session_id;
}


/**
 *
 */
void
BerkeleyDbFilesystem::create_node(DbTxn *txn, const String &name,
    bool ephemeral, uint64_t lock_generation,  uint32_t cur_lock_mode,
    uint64_t exclusive_handle)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp = 0;
  String buf;
  char numbuf[17];

  HT_DEBUG_OUT <<"create_node txn="<< txn <<" create node ='"<< name
               << " ephemeral=" << ephemeral << " lock_mode=" << cur_lock_mode
               << " lock_generation=" << lock_generation << " exclusive_handle="
               << " exclusive_handle" << HT_END;

  try {

    HT_EXPECT(!node_exists(txn, name), HYPERSPACE_STATEDB_NODE_EXISTS);
    // Store id under "/NODES/"
    String nodes_dir = NODES_STR;
    keym.set_str(nodes_dir);
    datam.set_str(name);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    cursorp->close();
    cursorp = 0;
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store node ephemeral
    key_str = get_node_key(name, NODE_EPHEMERAL);
    keym.set_str(key_str);
    if(ephemeral)
      buf = "1";
    else
      buf = "0";
    datam.set_str(buf);
    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);


    // Store node cur lock mode
    key_str = get_node_key(name, NODE_LOCK_MODE);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)cur_lock_mode);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store node lock generation
    key_str = get_node_key(name, NODE_LOCK_GENERATION);
    keym.set_str(key_str);
    sprintf(numbuf, "%llu", (Llu)lock_generation);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Store node exclusive_handle
    key_str = get_node_key(name, NODE_EXCLUSIVE_LOCK_HANDLE);
    keym.set_str(key_str);
    sprintf(numbuf, "%llu", (Llu)exclusive_handle);
    datam.set_str(numbuf);

    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT << "exitting create_node txn="<< txn <<" create node ='"
               << name << "' ephemeral=" << ephemeral << " lock_mode="
               << cur_lock_mode
               << " lock_generation=" << lock_generation << " exclusive_handle="
               << " exclusive_handle" << HT_END;

}

/**
 *
 */
void
BerkeleyDbFilesystem::set_node_lock_generation(DbTxn *txn, const String &name,
                                               uint64_t lock_generation)
{
  DbtManaged keym, datam;
  char numbuf[17];
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_node_lock_generation txn="<< txn <<" node=" << name
               <<" lock_generation=" << lock_generation << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing lock_generation
    key_str = get_node_key(name, NODE_LOCK_GENERATION);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    sprintf(numbuf, "%llu", (Llu)lock_generation);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_node_lock_generation txn="<< txn <<" node=" << name
               <<" lock_generation=" << lock_generation << HT_END;
}

/**
 *
 */
uint64_t
BerkeleyDbFilesystem::incr_node_lock_generation(DbTxn *txn, const String &name)
{
  DbtManaged keym, datam;
  char numbuf[17];
  int ret;
  uint64_t lock_generation=0;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"incr_node_lock_generation txn="<< txn <<" node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing lock_generation
    key_str = get_node_key(name, NODE_LOCK_GENERATION);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    lock_generation = (uint64_t)strtoull(datam.get_str(), 0, 0);
    ++lock_generation;
    sprintf(numbuf, "%llu", (Llu)lock_generation);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting incr_node_lock_generation txn="<< txn <<" node="<< name
               <<" lock_generation=" << lock_generation << HT_END;

  return lock_generation;
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_node_ephemeral(DbTxn *txn, const String &name,
                                         bool ephemeral)
{
  DbtManaged keym, datam;
  int ret;
  String buf;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_node_ephemeral txn="<< txn <<" node_name=" << name
               <<" ephemeral=" << ephemeral << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing node_name
    key_str = get_node_key(name, NODE_EPHEMERAL);
    keym.set_str(key_str);
    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);
    if(ephemeral)
      buf = "1";
    else
      buf = "0";
    datam.set_str(buf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting set_node_ephemeral txn="<< txn <<" node_name=" << name
               <<" ephemeral=" << ephemeral << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::node_is_ephemeral(DbTxn *txn, const String &name)
{
  DbtManaged keym, datam;
  int ret;
  String buf;
  String key_str;
  bool ephemeral;

  HT_DEBUG_OUT << "node_is_ephemeral txn=" << txn << " node_name=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Replace existing node_name
    key_str = get_node_key(name, NODE_EPHEMERAL);
    keym.set_str(key_str);
    ret = m_state_db->get(txn, &keym, &datam, 0);

    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    if (!strcmp(datam.get_str(), "0"))
      ephemeral = false;
    else if (!strcmp(datam.get_str(), "1"))
      ephemeral = true;
    else
      HT_EXPECT(false, HYPERSPACE_STATEDB_BAD_VALUE);

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting node_is_ephemeral txn=" << txn << " node_name= " << name
               << " ephemeral=" << ephemeral << HT_END;
  return ephemeral;
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_node_cur_lock_mode(DbTxn *txn, const String &name,
    uint32_t lock_mode)
{
  DbtManaged keym, datam;
  char numbuf[16];
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_node_cur_lock_mode txn="<< txn <<" node=" << name
               <<" lock_mode=" << lock_mode << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing lock_mode
    key_str = get_node_key(name, NODE_LOCK_MODE);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    sprintf(numbuf, "%lu", (Lu)lock_mode);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting set_node_cur_lock_mode txn="<< txn <<" node=" << name
               << " lock_mode=" << lock_mode << HT_END;
}

/**
 *
 */
uint32_t
BerkeleyDbFilesystem::get_node_cur_lock_mode(DbTxn *txn, const String &name)
{
  DbtManaged keym, datam;
  int ret;
  Dbc *cursorp = 0;
  String key_str;
  uint32_t lock_mode;

  HT_DEBUG_OUT <<"get_node_cur_lock_mode txn="<< txn <<" node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Get existing lock_mode
    key_str = get_node_key(name, NODE_LOCK_MODE);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    lock_mode = (uint32_t) strtoull(datam.get_str(), 0, 0);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting get_node_cur_lock_mode txn="<< txn <<" node=" << name
               << " lock_mode=" << lock_mode << HT_END;
  return lock_mode;
}

/**
 *
 */
void
BerkeleyDbFilesystem::set_node_exclusive_lock_handle(DbTxn *txn,
    const String &name, uint64_t exclusive_lock_handle)
{
  DbtManaged keym, datam;
  char numbuf[17];
  int ret;
  Dbc *cursorp = 0;
  String key_str;

  HT_DEBUG_OUT <<"set_node_exclusive_lock_handle  txn="<< txn <<" node=" << name
               <<" exclusive_lock_handle=" << exclusive_lock_handle << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing exclusive lock handle
    key_str = get_node_key(name, NODE_EXCLUSIVE_LOCK_HANDLE);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    sprintf(numbuf, "%llu", (Llu)exclusive_lock_handle);
    datam.set_str(numbuf);

    ret = cursorp->put(&keym, &datam, DB_CURRENT);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting set_node_exclusive_lock_handle  txn="<< txn <<" node="<< name
               <<" exclusive_lock_handle=" << exclusive_lock_handle << HT_END;
}

/**
 *
 */
uint64_t
BerkeleyDbFilesystem::get_node_exclusive_lock_handle(DbTxn *txn, const String &name)
{
  DbtManaged keym, datam;
  int ret;
  Dbc *cursorp = 0;
  String key_str;
  uint64_t exclusive_lock_handle=0;

  HT_DEBUG_OUT <<"get_node_exclusive_lock_handle  txn="<< txn <<" node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    m_state_db->cursor(txn, &cursorp, 0);

    // Replace existing exclusive lock handle
    key_str = get_node_key(name, NODE_EXCLUSIVE_LOCK_HANDLE);
    keym.set_str(key_str);

    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND);

    exclusive_lock_handle = (uint64_t)strtoull(datam.get_str(), 0, 0);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting get_node_exclusive_lock_handle  txn=" << txn <<" node=" << name
               <<" exclusive_lock_handle=" << exclusive_lock_handle << HT_END;

  return exclusive_lock_handle;
}

/**
 *
 */
void
BerkeleyDbFilesystem::add_node_handle(DbTxn *txn, const String &name,
    uint64_t handle_id)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"add_node_handle txn="<< txn <<" node="<< name << " handle id=" << handle_id
               << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    String node_handles_dir = get_node_key(name,
        NODE_HANDLE_MAP);
    keym.set_str(node_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting add_node_handle txn="<< txn <<" node="<< name
               << " handle id=" << handle_id << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::get_node_event_notification_map(DbTxn *txn, const String &name,
    uint32_t event_mask, NotificationMap &handles_to_sessions)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  uint64_t handle, session;
  uint32_t mask;
  Dbc *cursorp = 0;
  bool has_notifications = false;

  HT_DEBUG_OUT <<"get_node_event_notification_map txn="<< txn <<" node="<< name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    String node_handles_dir = get_node_key(name, NODE_HANDLE_MAP);
    keym.set_str(node_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);

    // iterate through all handles
    while (ret != DB_NOTFOUND) {
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
      handle  = (uint64_t)strtoull(datam.get_str(), 0, 0);
      mask = get_handle_event_mask(txn, handle);
      // if this handle was registered for this event then store it in the notification map
      if ((mask&event_mask) != 0) {
        session = get_handle_session(txn, handle);
        handles_to_sessions[handle] = session;
        has_notifications = true;
      }
      ret = cursorp->get(&keym, &datam, DB_NEXT_DUP);
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "exitting get_node_event_notification_map txn=" << txn << " node="<< name
               << " has_notifications=" << has_notifications << HT_END;
  return has_notifications;
}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_node_handle(DbTxn *txn, const String &name,
    uint64_t handle_id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp;
  char numbuf[17];

  HT_DEBUG_OUT <<"delete_node_handle txn="<< txn <<" node="<< name
               <<" handle_id=" << handle_id << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Delete handle
    String node_handles_dir = get_node_key(name, NODE_HANDLE_MAP);
    keym.set_str(node_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);
    HT_EXPECT(ret ==0 , HYPERSPACE_STATEDB_ERROR);

    ret = cursorp->del(0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting delete_node_handle txn="<< txn <<" node="<< name
               <<" handle_id=" << handle_id << HT_END;
}



/**
 *
 */
void
BerkeleyDbFilesystem::add_node_pending_lock_request(DbTxn *txn,
    const String &name, uint64_t handle_id, uint32_t mode)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[17];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"add_node_pending_lock_request txn="<< txn <<" node=" << name
               << " handle id=" << handle_id << " mode=" << mode << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    String node_pending_locks_dir = get_node_key(name, NODE_PENDING_LOCK_REQUESTS);
    keym.set_str(node_pending_locks_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;

    key_str = get_node_pending_lock_request_key(name, handle_id);
    keym.set_str(key_str);
    sprintf(numbuf, "%lu", (Lu)mode);
    datam.set_str(numbuf);
    ret = m_state_db->put(txn, &keym, &datam, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<" exitting add_node_pending_lock_request txn="<< txn << " node=" << name
               <<" handle id=" << handle_id << " mode=" << mode << HT_END;
}

/**
 *
 */

bool
BerkeleyDbFilesystem::node_has_pending_lock_request(DbTxn *txn, const String &name)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  bool has_pending_lock_request = false;
  Dbc *cursorp = 0;
  uint64_t handle_id;

  HT_DEBUG_OUT << "node_has_pending_lock_request txn=" << txn << " node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // get top pending handle id of lock request (if any)
    String node_pending_locks_dir = get_node_key(name, NODE_PENDING_LOCK_REQUESTS);
    keym.set_str(node_pending_locks_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);

    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;

    if (ret == 0) {
      // get lock request mode
      handle_id = (uint64_t) strtoull(datam.get_str(), 0, 0);
      key_str = get_node_pending_lock_request_key(name, handle_id);
      keym.set_str(key_str);
      ret = m_state_db->get(txn, &keym, &datam, 0);
      HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
      has_pending_lock_request = true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<" exitting node_has_pending_lock_request txn="<< txn << " node=" << name
               <<" node_has_pending_lock_request=" << has_pending_lock_request << HT_END;

  return has_pending_lock_request;
}

/**
 *
 */

bool
BerkeleyDbFilesystem::get_node_pending_lock_request(DbTxn *txn, const String &name,
    LockRequest &front_req)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  bool has_pending_lock_request = false;
  Dbc *cursorp = 0;
  uint64_t handle_id;

  HT_DEBUG_OUT << "get_node_pending_lock_request txn=" << txn << " node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // get top pending handle id of lock request (if any)
    String node_pending_locks_dir = get_node_key(name, NODE_PENDING_LOCK_REQUESTS);
    keym.set_str(node_pending_locks_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);

    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;

    if (ret == 0) {
      // get lock request mode
      has_pending_lock_request = true;
      handle_id = (uint64_t) strtoull(datam.get_str(), 0, 0);
      key_str = get_node_pending_lock_request_key(name, handle_id);
      keym.set_str(key_str);
      ret = m_state_db->get(txn, &keym, &datam, 0);
      HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

      front_req.handle = handle_id;
      front_req.mode = (uint32_t) strtoull(datam.get_str(), 0, 0);
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<" exitting get_node_pending_lock_request txn="<< txn << " node=" << name
               <<" has_pending_lock_request=" << has_pending_lock_request << HT_END;

  return has_pending_lock_request;
}
/**
 *
 */
void
BerkeleyDbFilesystem::delete_node_pending_lock_request(DbTxn *txn,
    const String &name, uint64_t handle_id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"remove_node_pending_lock_request txn="<< txn <<" node=" << name
               <<" handle id=" << handle_id << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Delete node pending lock req
    String node_pending_locks_dir = get_node_key(name,
        NODE_PENDING_LOCK_REQUESTS);
    keym.set_str(node_pending_locks_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);
    HT_EXPECT(ret ==0 , HYPERSPACE_STATEDB_ERROR);

    ret = cursorp->del(0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
    //Delete pending lock request
    key_str = get_node_pending_lock_request_key(name, handle_id);
    keym.set_str(key_str);
    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting remove_node_pending_lock_request txn="<< txn
               <<" node=" << name << " handle id=" << handle_id << HT_END;
}

/**
 *
 */
void
BerkeleyDbFilesystem::add_node_shared_lock_handle(DbTxn *txn, const String &name,
                                                  uint64_t handle_id)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  char numbuf[16];
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"add_node_shared_lock_handle txn="<< txn <<" node="<< name
      << " handle id=" << handle_id << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    String node_shared_handles_dir = get_node_key(name,
        NODE_SHARED_LOCK_HANDLES);
    keym.set_str(node_shared_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->put(&keym, &datam, DB_KEYLAST);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting add_node_shared_lock_handle txn="<< txn <<" node="<< name
               << " handle id=" << handle_id << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::node_has_shared_lock_handles(DbTxn *txn, const String &name)
{
  int ret;
  DbtManaged  keym, datam;
  String key_str;
  Dbc *cursorp = 0;
  bool has_shared_lock_handles=false;

  HT_DEBUG_OUT <<"node_has_shared_lock_handles txn="<< txn <<" node="<< name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    String node_shared_handles_dir = get_node_key(name, NODE_SHARED_LOCK_HANDLES);
    keym.set_str(node_shared_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);
    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);

    if (ret != DB_NOTFOUND)
      has_shared_lock_handles = true;

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"exitting node_has_shared_lock_handle txn="<< txn <<" node="<< name
               <<" has_shared_lock_handles=" << has_shared_lock_handles << HT_END;
  return has_shared_lock_handles;
}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_node_shared_lock_handle(DbTxn *txn, const String &name,
                                                     uint64_t handle_id)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp;
  char numbuf[17];

  HT_DEBUG_OUT <<"remove_node_shared_lock_handle txn="<< txn <<" node="<< name
               <<" handle_id=" << handle_id << HT_END;
  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Delete shared lock handle
    String node_shared_handles_dir = get_node_key(name, NODE_SHARED_LOCK_HANDLES );
    keym.set_str(node_shared_handles_dir);
    sprintf(numbuf, "%llu", (Llu)handle_id);
    datam.set_str(numbuf);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);
    HT_EXPECT(ret ==0 , HYPERSPACE_STATEDB_ERROR);

    ret = cursorp->del(0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT <<"exitting remove_node_shared_lock_handle txn="<< txn <<" node="<< name
               <<" handle_id=" << handle_id << HT_END;
}

/**
 *
 */
void
BerkeleyDbFilesystem::delete_node(DbTxn *txn, const String &name)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp = 0;

  HT_DEBUG_OUT <<"delete_node txn="<< txn <<" node ="<< name << HT_END;
  try {

    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);
    // Delete name under "/NODES/"
    String nodes_dir = NODES_STR;
    keym.set_str(nodes_dir);
    datam.set_str(name);

    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_GET_BOTH);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    ret = cursorp->del(0);
    HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);

    // Delete node ephemeral bool
    key_str = get_node_key(name, NODE_EPHEMERAL);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete node cur_lock_mode
    key_str = get_node_key(name, NODE_LOCK_MODE);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete node lock_generation
    key_str = get_node_key(name, NODE_LOCK_GENERATION);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    // Delete node exclusive_lock_handle
    key_str = get_node_key(name, NODE_EXCLUSIVE_LOCK_HANDLE);
    keym.set_str(key_str);

    ret = m_state_db->del(txn, &keym, 0);
    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    cursorp->close();
    cursorp = 0;

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }
  HT_DEBUG_OUT << "exitting delete_node txn="<< txn <<" node="
      << name << HT_END;

}


/**
 *
 */
bool
BerkeleyDbFilesystem::node_exists(DbTxn *txn, const String &name)
{
  DbtManaged keym, datam;
  Dbc *cursorp = 0;
  bool exists = true;

  HT_DEBUG_OUT <<"node_exists txn="<< txn << " node name="
      << name << HT_END;

  try {
    m_state_db->cursor(txn, &cursorp, 0);

    // Check for id under "/NODES/"
    String nodes_dir = NODES_STR;
    keym.set_str(nodes_dir);
    datam.set_str(name);

    if(cursorp->get(&keym, &datam, DB_GET_BOTH) == DB_NOTFOUND) {
      exists = false;
    }
    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
   }

  HT_DEBUG_OUT <<"exitting node_exists txn="<< txn << " node name =" << name
               <<" exists=" << exists << HT_END;
  return exists;
}

/**
 *
 */
void
BerkeleyDbFilesystem::get_node_handles(DbTxn *txn, const String &name,
                                       vector<uint64_t> &handles)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp;

  HT_DEBUG_OUT << "get_node_handles txn=" << txn << " node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Iterate through all handles
    String node_handles_dir = get_node_key(name, NODE_HANDLE_MAP);
    keym.set_str(node_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);

    while (ret != DB_NOTFOUND) {
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
      handles.push_back((uint64_t)strtoull(datam.get_str(), 0, 0));
      ret = cursorp->get(&keym, &datam, DB_NEXT_DUP);
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "get_node_handles txn=" << txn << " node=" << name << HT_END;
}

/**
 *
 */
bool
BerkeleyDbFilesystem::node_has_open_handles(DbTxn *txn, const String &name)
{
  int ret;
  DbtManaged keym, datam;
  String key_str;
  Dbc *cursorp;
  bool has_open_handles = false;
  uint64_t open_handle;

  HT_DEBUG_OUT << "node_has_open_handles txn=" << txn << " node=" << name << HT_END;

  try {
    HT_EXPECT(node_exists(txn, name), HYPERSPACE_STATEDB_NODE_NOT_EXISTS);

    // Check to see if there is even one handle open to this node
    String node_handles_dir = get_node_key(name, NODE_HANDLE_MAP);
    keym.set_str(node_handles_dir);
    m_state_db->cursor(txn, &cursorp, 0);
    ret = cursorp->get(&keym, &datam, DB_SET);

    HT_EXPECT(ret == 0 || ret == DB_NOTFOUND, HYPERSPACE_STATEDB_ERROR);
    if (ret == 0) {
      open_handle = (uint64_t) strtoull((const char *)datam.get_data(), 0, 0);
      HT_DEBUG_OUT << "node_has_open_handles txn=" << txn << " node=" << name
                   << " at least one open_handle=" << open_handle << HT_END;
      has_open_handles = true;
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT << "node_has_open_handles txn=" << txn << " node=" << name
               << " has_open_handles=" << has_open_handles << HT_END;
  return has_open_handles;
}

/**
 *
 */
uint64_t
BerkeleyDbFilesystem::get_next_id_i64(DbTxn *txn, int id_type, bool increment)
{
  DbtManaged keym, datam;
  int ret;
  uint64_t retval=0;
  Dbc *cursorp = 0;
  char numbuf[17];

  HT_DEBUG_OUT <<"get_next_id_i64 txn="<< txn << " id_type=" << id_type << " increment="
               << increment << HT_END;
  try {
    m_state_db->cursor(txn, &cursorp, 0);

    // Get next id
    switch (id_type) {
      case SESSION_ID:
        keym.set_str(NEXT_SESSION_ID);
        break;
      case HANDLE_ID:
        keym.set_str(NEXT_HANDLE_ID);
        break;
      case EVENT_ID:
        keym.set_str(NEXT_EVENT_ID);
        break;
      default:
        HT_THROWF(HYPERSPACE_STATEDB_BAD_KEY, "Unknown i64 id type:%d",id_type);
    }

    ret = cursorp->get(&keym, &datam, DB_SET);

    HT_EXPECT(ret == 0, HYPERSPACE_STATEDB_ERROR);

    retval = strtoull((const char *)datam.get_data(), 0, 0);
    if (increment) {
      sprintf(numbuf, "%llu", (Llu)retval+1);
      datam.set_str(numbuf);
      ret = cursorp->put(&keym, &datam, DB_CURRENT);
      HT_EXPECT(ret==0, HYPERSPACE_STATEDB_ERROR);
    }

    cursorp->close();
    cursorp = 0;
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if(cursorp)
      cursorp->close();
    if (e.get_errno() == DB_LOCK_DEADLOCK)
      HT_THROW(HYPERSPACE_BERKELEYDB_DEADLOCK, e.what());
    else
      HT_THROW(HYPERSPACE_STATEDB_ERROR, e.what());
  }

  HT_DEBUG_OUT <<"get_next_id_i64 txn="<< txn << " id_type=" << id_type << " increment="
               << increment << " retval=" << retval << HT_END;
  return retval;
}
