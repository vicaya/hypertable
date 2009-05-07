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

#ifndef HT_BERKELEYDBFILESYSTEM_H
#define HT_BERKELEYDBFILESYSTEM_H

#include <vector>

#include <db_cxx.h>

#include "Common/String.h"
#include "Common/DynamicBuffer.h"

#include "DirEntry.h"

namespace Hyperspace {
  using namespace Hypertable;

  class BerkeleyDbFilesystem {
  public:
    BerkeleyDbFilesystem(const String &basedir, bool force_recover=false);
    ~BerkeleyDbFilesystem();

    DbTxn *start_transaction();

    bool get_xattr_i32(DbTxn *txn, const String &fname,
                       const String &aname, uint32_t *valuep);
    void set_xattr_i32(DbTxn *txn, const String &fname,
                       const String &aname, uint32_t value);
    bool get_xattr_i64(DbTxn *txn, const String &fname,
                       const String &aname, uint64_t *valuep);
    void set_xattr_i64(DbTxn *txn, const String &fname,
                       const String &aname, uint64_t value);
    void set_xattr(DbTxn *txn, const String &fname,
                   const String &aname, const void *value, size_t value_len);
    bool get_xattr(DbTxn *txn, const String &fname, const String &aname,
                   Hypertable::DynamicBuffer &vbuf);
    bool exists_xattr(DbTxn *txn, const String &fname, const String &aname);
    void del_xattr(DbTxn *txn, const String &fname, const String &aname);
    void mkdir(DbTxn *txn, const String &name);
    void unlink(DbTxn *txn, const String &name);
    bool exists(DbTxn *txn, String fname, bool *is_dir_p=0);
    void create(DbTxn *txn, const String &fname, bool temp);
    void get_directory_listing(DbTxn *txn, String fname,
                               std::vector<DirEntry> &listing);
    void get_all_names(DbTxn *txn, std::vector<String> &names);

    bool list_xattr(DbTxn *txn, const String& fname, std::vector<String> &anames);

    static const char NODE_ATTR_DELIM = 0x01;

  private:
    void build_attr_key(DbTxn *, String &keystr,
                        const String &aname, Dbt &key);

    String m_base_dir;
    DbEnv  m_env;
    Db    *m_db;

  };

} // namespace Hyperspace

#endif // HT_BERKELEYDBFILESYSTEM_H
