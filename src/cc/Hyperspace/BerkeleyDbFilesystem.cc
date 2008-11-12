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

    m_env.set_lk_detect(DB_LOCK_DEFAULT);

    m_env.open(m_base_dir.c_str(), env_flags, 0);
    m_db = new Db(&m_env, 0);
    m_db->open(NULL, "namespace.db", NULL, DB_BTREE, db_flags, 0);

    txn = start_transaction();

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
    delete m_db;
    m_env.close(0);
  }
  catch(DbException &e) {
    HT_ERRORF("Error closing Berkeley DB (dir=%s) - %s",
              m_base_dir.c_str(), e.what());
  }
  HT_DEBUG_OUT <<"namespace closed"<< HT_END;
}


DbTxn *BerkeleyDbFilesystem::start_transaction() {
  DbTxn *txn = NULL;

  // begin transaction
  try {
    m_env.txn_begin(NULL, &txn, DB_READ_COMMITTED);
  }
  catch (DbException &e) {
    HT_FATALF("Error starting Berkeley DB transaction: %s", e.what());
  }
  HT_DEBUG_OUT <<"txn="<< txn << HT_END;

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
