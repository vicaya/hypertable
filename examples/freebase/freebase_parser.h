/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef FREEBASE_PARSER_H
#define FREEBASE_PARSER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>

#include "Hypertable/Lib/KeySpec.h"

struct InsertRec {
  Hypertable::KeySpec key;
  const void *value;
  uint32_t value_len;
};

struct ColumnInfo {
  const char *name;
  size_t name_len;
  const char *value;
  size_t value_len;
};

class freebase_parser {

 public:
  freebase_parser();
  bool load(const std::string fname);
  InsertRec *next(int *countp);

 private:
  std::string m_fname;
  std::ifstream m_fin;
  std::string m_header_line;
  std::string m_line;
  std::vector<ColumnInfo> m_column_info;
  InsertRec *m_inserts;
  std::string m_category;
  int m_column_name;
  int m_column_id;
  int m_lineno;
};

#endif // FREEBASE_PARSER_H
