/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_LOADDATASOURCEFACTORY_H
#define HYPERTABLE_LOADDATASOURCEFACTORY_H

#include "Common/String.h"

#include "LoadDataSource.h"

namespace Hypertable {

  class LoadDataSourceFactory {

  public:

    static LoadDataSource *create(const String &fname, const int src,
                                  const String &header_fname, const int header_src,
                                  const std::vector<String> &key_columns,
                                  const String &timestamp_column,
                                  int row_uniquify_chars = 0,
                                  int load_flags = 0);
  };

} // namespace Hypertable

#endif // HYPERTABLE_LOADDATASOURCEFACTORY_H
