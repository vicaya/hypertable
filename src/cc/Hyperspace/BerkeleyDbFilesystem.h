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

#ifndef BERKELEYDBFILESYSTEM_H
#define BERKELEYDBFILESYSTEM_H

#include <vector>

#include <db_cxx.h>

#include "Common/DynamicBuffer.h"

#include "DirEntry.h"

namespace Hyperspace {

  class BerkeleyDbFilesystem {
  public:
    BerkeleyDbFilesystem(const std::string &basedir);
    ~BerkeleyDbFilesystem();

    DbTxn *start_transaction();

    bool get_xattr_i32(DbTxn *txn, std::string fname, const std::string &aname, uint32_t *valuep);
    void set_xattr_i32(DbTxn *txn, std::string fname, const std::string &aname, uint32_t value);
    bool get_xattr_i64(DbTxn *txn, std::string fname, const std::string &aname, uint64_t *valuep);
    void set_xattr_i64(DbTxn *txn, std::string fname, const std::string &aname, uint64_t value);
    void set_xattr(DbTxn *txn, std::string fname, const std::string &aname, const void *value, size_t value_len);
    bool get_xattr(DbTxn *txn, std::string fname, const std::string &aname, Hypertable::DynamicBuffer &vbuf);
    void del_xattr(DbTxn *txn, std::string fname, const std::string &aname);
    void mkdir(DbTxn *txn, const std::string &name);
    void unlink(DbTxn *txn, const std::string &name);
    bool exists(DbTxn *txn, std::string fname, bool *is_dir_p=0);
    void create(DbTxn *txn, const std::string &fname, bool temp);
    void get_directory_listing(DbTxn *txn, std::string fname, std::vector<DirEntryT> &listing);
    void get_all_names(DbTxn *txn, std::vector<std::string> &names);

  private:
    std::string m_base_dir;
    DbEnv  m_env;
    Db    *m_db;
  };

}

#endif // BERKELEYDBFILESYSTEM_H
