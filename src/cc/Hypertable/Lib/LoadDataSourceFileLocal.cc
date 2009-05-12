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

#include "Common/Compat.h"
#include <fstream>
#include <iostream>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/FileUtils.h"

#include "Key.h"
#include "LoadDataSourceFileLocal.h"

using namespace boost::iostreams;
using namespace Hypertable;
using namespace std;

/**
 *
 */
LoadDataSourceFileLocal::LoadDataSourceFileLocal(const String &fname,
    const String &header_fname, int row_uniquify_chars, bool dupkeycols)
  : LoadDataSource(row_uniquify_chars, dupkeycols),
    m_source(fname), m_header_fname(header_fname) {

  if (!FileUtils::exists(fname.c_str()))
    HT_THROW(Error::FILE_NOT_FOUND, fname);

  if (boost::algorithm::ends_with(fname, ".gz")) {
    m_fin.push(gzip_decompressor());
    m_zipped = true;
  }

  return;
}

void
LoadDataSourceFileLocal::init_src()
{

  m_fin.push(m_source);
}

String
LoadDataSourceFileLocal::get_header()
{
  String header = "";
  if (m_header_fname != "") {
    std::ifstream in(m_header_fname.c_str());
    getline(in, header);
  }
  else {
    getline(m_fin, header);
  }

  return header;
}

/**
 *
 */
uint64_t
LoadDataSourceFileLocal::incr_consumed()
{
  uint64_t consumed=0;
  uint64_t new_offset = m_source.seek(0, BOOST_IOS::cur);
  consumed = new_offset - m_offset;
  m_offset = new_offset;
  return consumed;
}

