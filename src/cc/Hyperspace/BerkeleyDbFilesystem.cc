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

#include <vector>

#include <boost/algorithm/string.hpp>

#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/String.h"

#include "BerkeleyDbFilesystem.h"
#include "DbtManaged.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 */
BerkeleyDbFilesystem::BerkeleyDbFilesystem(const std::string &basedir) : m_base_dir(basedir), m_env(0) {
  DbTxn *txn = NULL;

  u_int32_t env_flags = 
    DB_CREATE |      // If the environment does not exist, create it
    DB_INIT_LOCK  |  // Initialize locking 
    DB_INIT_LOG   |  // Initialize logging 
    DB_INIT_MPOOL |  // Initialize the cache 
    DB_INIT_TXN |    // Initialize transactions 
    DB_THREAD;
  u_int32_t db_flags = DB_CREATE | DB_AUTO_COMMIT | DB_THREAD;

  /**
   * Open Berkeley DB environment and namespace database
   */
  try { 
    int ret;
    Dbt key, data;

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
    if (txn)
      txn->abort();
    HT_FATALF("Error initializing Berkeley DB (dir=%s) - %s", m_base_dir.c_str(), e.what());
  }  
  
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
    HT_ERRORF("Error closing Berkeley DB (dir=%s) - %s", m_base_dir.c_str(), e.what());
  }

}



DbTxn *BerkeleyDbFilesystem::start_transaction() {
  DbTxn *txn = NULL;

  // begin transaction
  try {
    m_env.txn_begin(NULL, &txn, 0); 
  }
  catch (DbException &e) {
    HT_FATALF("Error starting Berkeley DB transaction: %s", e.what());
  } 

  return txn;
}



/**
 */
bool BerkeleyDbFilesystem::get_xattr_i32(DbTxn *txn, std::string fname, const std::string &aname, uint32_t *valuep) {
  int ret;
  Dbt key;
  DbtManaged data;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  try {

    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      uint8_t *ptr = (uint8_t *)data.get_data();
      size_t remaining = data.get_size();
      *valuep = Serialization::decode_i32((const uint8_t **)&ptr, &remaining);
      return true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  return false;
}



/**
 */
void BerkeleyDbFilesystem::set_xattr_i32(DbTxn *txn, std::string fname, const std::string &aname, uint32_t value) {
  int ret;
  Dbt key, data;
  uint32_t uval;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  uint8_t *ptr = (uint8_t *)&uval;
  Serialization::encode_i32(&ptr, value);

  data.set_data(&uval);
  data.set_size(4);

  try {
    ret = m_db->put(txn, &key, &data, 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  assert(ret == 0);

  return;
}


/**
 */
bool BerkeleyDbFilesystem::get_xattr_i64(DbTxn *txn, std::string fname, const std::string &aname, uint64_t *valuep) {
  int ret;
  Dbt key;
  DbtManaged data;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  try {
  
    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      uint8_t *ptr = (uint8_t *)data.get_data();
      size_t remaining = data.get_size();
      *valuep = Serialization::decode_i64((const uint8_t **)&ptr, &remaining);
      return true;
    }

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  return false;
}



/**
 */
void BerkeleyDbFilesystem::set_xattr_i64(DbTxn *txn, std::string fname, const std::string &aname, uint64_t value) {
  int ret;
  Dbt key, data;
  uint64_t uval;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  uint8_t *ptr = (uint8_t *)&uval;
  Serialization::encode_i64(&ptr, value);

  data.set_data(&uval);
  data.set_size(8);

  try {
    ret = m_db->put(txn, &key, &data, 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  assert(ret == 0);

  return;
}



/**
 */
void BerkeleyDbFilesystem::set_xattr(DbTxn *txn, std::string fname, const std::string &aname, const void *value, size_t value_len) {
  int ret;
  Dbt key, data;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  data.set_data((void *)value);
  data.set_size(value_len);

  try {
    ret = m_db->put(txn, &key, &data, 0);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  assert(ret == 0);

  return;  
}



/**
 */
bool BerkeleyDbFilesystem::get_xattr(DbTxn *txn, std::string fname, const std::string &aname, DynamicBuffer &vbuf) {
  int ret;
  Dbt key;
  DbtManaged data;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  try {
    if ((ret = m_db->get(txn, &key, &data, 0)) == 0) {
      vbuf.reserve(data.get_size());
      memcpy(vbuf.base, (uint8_t *)data.get_data(), data.get_size());
      vbuf.ptr += data.get_size();
      return true;
    }
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

  return false;
}



void BerkeleyDbFilesystem::del_xattr(DbTxn *txn, std::string fname, const std::string &aname) {
  int ret;
  Dbt key;
  bool isdir;

  if (!exists(txn, fname, &isdir))
    throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, fname);

  if (isdir)
    fname += "/";

  std::string key_str = fname + ":" + aname;

  key.set_data((void *)key_str.c_str());
  key.set_size(key_str.length()+1);

  try {
    if ((ret = m_db->del(txn, &key, 0)) == DB_NOTFOUND)
      throw Exception(Error::HYPERSPACE_ATTR_NOT_FOUND, aname);
  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

}



/**
 */
void BerkeleyDbFilesystem::mkdir(DbTxn *txn, const std::string &name) {
  int ret;
  Dbt key;
  DbtManaged data;
  size_t lastslash = name.rfind('/', name.length()-1);
  std::string dirname = name.substr(0, lastslash+1);

  try {

    /**
     * Make sure parent directory exists
     */
    key.set_data((void *)dirname.c_str());
    key.set_size(dirname.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND)
      throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, dirname);

    // formulate directory name
    dirname = name;
    if (dirname[dirname.length()-1] != '/')
      dirname += "/";

    /**
     * Make sure directory does not already exists
     */
    key.set_data((void *)dirname.c_str());
    key.set_size(dirname.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) != DB_NOTFOUND)
      throw Exception(Error::HYPERSPACE_FILE_EXISTS, dirname);

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
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 
  
}


void BerkeleyDbFilesystem::unlink(DbTxn *txn, const std::string &name) {
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
	    if (str.length() > name.length()+1 && str[name.length()+1] != ':') {
	      cursorp->close();
	      throw Exception(Error::HYPERSPACE_DIR_NOT_EMPTY, name);
	    }
	    looks_like_dir = true;
	    delkeys.push_back(keym.get_str());
	  }
	  else if (str[name.length()] == ':') {
	    looks_like_file = true;
	    delkeys.push_back(keym.get_str());
	  }
	}
	else {
	  delkeys.push_back(keym.get_str());
	  looks_like_file = true;
	}

      } while (cursorp->get(&keym, &datam, DB_NEXT) != DB_NOTFOUND &&
	       boost::algorithm::starts_with(keym.get_str(), name.c_str()));

    }

    cursorp->close();
    cursorp = 0;

    HT_EXPECT(!(looks_like_dir && looks_like_file), Error::FAILED_EXPECTATION);

    if (delkeys.empty())
      throw Exception(Error::HYPERSPACE_FILE_NOT_FOUND, name);

    for (size_t i=0; i<delkeys.size(); i++) {
      key.set_data((void *)delkeys[i].c_str());
      key.set_size(delkeys[i].length()+1);
      HT_EXPECT(m_db->del(txn, &key, 0) != DB_NOTFOUND, Error::FAILED_EXPECTATION);
    }

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 

}


bool BerkeleyDbFilesystem::exists(DbTxn *txn, std::string fname, bool *is_dir_p) {
  int ret;
  Dbt key;
  DbtManaged data;

  if (is_dir_p)
    *is_dir_p = false;

  key.set_data((void *)fname.c_str());
  key.set_size(fname.length()+1);

  try {

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND) {
      fname += "/";
      key.set_data((void *)fname.c_str());
      key.set_size(fname.length()+1);
      if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND)
	return false;
      if (is_dir_p)
	*is_dir_p = true;
    }

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  }

  return true;
}


/**
 *
 */
void BerkeleyDbFilesystem::create(DbTxn *txn, const std::string &fname, bool temp) {
  int ret;
  Dbt key;
  DbtManaged data;

  try {

    if (exists(txn, fname, 0))
      throw Exception(Error::HYPERSPACE_FILE_EXISTS, fname);

    if (boost::algorithm::ends_with(fname, "/"))
      throw Exception(Error::HYPERSPACE_IS_DIRECTORY, fname);

    size_t lastslash = fname.rfind('/', fname.length()-1);
    std::string parent_dir = fname.substr(0, lastslash+1);

    key.set_data((void *)parent_dir.c_str());
    key.set_size(parent_dir.length()+1);

    if ((ret = m_db->get(txn, &key, &data, 0)) == DB_NOTFOUND)
      throw Exception(Error::HYPERSPACE_BAD_PATHNAME, fname);

    key.set_data((void *)fname.c_str());
    key.set_size(fname.length()+1);

    ret = m_db->put(txn, &key, &data, 0);

    if (temp) {
      std::string temp_key = fname + ":temp";
      key.set_data((void *)temp_key.c_str());
      key.set_size(temp_key.length()+1);
      ret = m_db->put(txn, &key, &data, 0);
    }

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 
  
}



void BerkeleyDbFilesystem::get_directory_listing(DbTxn *txn, std::string fname, std::vector<DirEntryT> &listing) {
  DbtManaged keym, datam;
  Dbt key;
  Dbc *cursorp = 0;
  String str, last_str;
  DirEntryT entry;
  size_t offset;

  try {

    m_db->cursor(txn, &cursorp, 0);

    if (!boost::algorithm::ends_with(fname, "/"))
      fname += "/";

    keym.set_str(fname);

    if (cursorp->get(&keym, &datam, DB_SET_RANGE) != DB_NOTFOUND) {

      if (!boost::algorithm::starts_with(keym.get_str(), fname.c_str())) {
	cursorp->close();
	throw Exception(Error::HYPERSPACE_BAD_PATHNAME, fname);
      }

      do {

	str = keym.get_str();

	if (str.length() > fname.length()) {
	  if (str[fname.length()] != ':') {
	    str = str.substr(fname.length());
	    if ((offset = str.find('/')) != std::string::npos) {
	      entry.name = str.substr(0, offset);
	      if (entry.name != last_str) {
		entry.isDirectory = true;
		listing.push_back(entry);
		last_str = entry.name;
	      }
	    }
	    else {
	      if ((offset = str.find(':')) != std::string::npos) {
		entry.name = str.substr(0, offset);
		if (entry.name != last_str) {
		  entry.isDirectory = false;
		  listing.push_back(entry);
		  last_str = entry.name;
		}
	      }
	      else {
		entry.name = str;
		if (entry.name != last_str) {
		  entry.isDirectory = false;
		  listing.push_back(entry);
		  last_str = entry.name;
		}
	      }
	    }
	  }
	}

      } while (cursorp->get(&keym, &datam, DB_NEXT) != DB_NOTFOUND &&
	       boost::algorithm::starts_with(keym.get_str(), fname.c_str()));

    }

    cursorp->close();
    cursorp = 0;

  }
  catch (DbException &e) {
    HT_ERRORF("Berkeley DB error: %s", e.what());
    if (cursorp)
      cursorp->close();
    throw Exception(Error::HYPERSPACE_BERKELEYDB_ERROR, e.what());
  } 
  
}


void BerkeleyDbFilesystem::get_all_names(DbTxn *txn, std::vector<std::string> &names) {
  DbtManaged keym, datam;
  Dbc *cursorp = NULL;
  int ret;

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
