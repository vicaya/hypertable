/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_THRIFTHELPER_H
#define HYPERTABLE_THRIFTHELPER_H

#include <iosfwd>
#include "gen-cpp/HqlService.h"

namespace Hypertable { namespace ThriftGen {

std::ostream &operator<<(std::ostream &, const RowInterval &);
std::ostream &operator<<(std::ostream &, const Cell &);
std::ostream &operator<<(std::ostream &, const CellAsArray &);
std::ostream &operator<<(std::ostream &, const CellInterval &);
std::ostream &operator<<(std::ostream &, const ScanSpec &);
std::ostream &operator<<(std::ostream &, const HqlResult &);
std::ostream &operator<<(std::ostream &, const ClientException &);

// These are mostly for test code and not efficient for production.
Cell
make_cell(const char *row, const char *cf, const char *cq,
          const std::string &value, int64_t ts, int64_t rev, CellFlag);

Cell
make_cell(const char *row, const char *cf, const char *cq = 0,
          const std::string &value = std::string(), const char *ts = 0,
          const char *rev = 0, CellFlag = INSERT);

}} // namespace Hypertable::Thrift

#endif /* HYPERTABLE_THRIFTHELPER_H */
