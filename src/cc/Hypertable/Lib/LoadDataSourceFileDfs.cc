/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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
#include "LoadDataSourceFileDfs.h"

using namespace boost::iostreams;
using namespace Hypertable;
using namespace std;

/**
 *
 */
LoadDataSourceFileDfs::LoadDataSourceFileDfs(DfsBroker::ClientPtr &client,
  const String &fname, const String &header_fname, int row_uniquify_chars, int load_flags)
  : LoadDataSource(header_fname, row_uniquify_chars, load_flags), m_cur_offset(0) {

  HT_ASSERT(client);
  m_source = new DfsBroker::FileSource(client, fname);

  if (boost::algorithm::ends_with(fname, ".gz")) {
    m_fin.push(gzip_decompressor());
    m_zipped = true;
  }

  return;
}

void
LoadDataSourceFileDfs::init_src()
{
  m_fin.push(*m_source);
  m_source_size = m_source->length();
}

/**
 *
 */
uint64_t
LoadDataSourceFileDfs::incr_consumed()
{
  size_t new_offset = m_source->bytes_read();
  uint64_t consumed = new_offset - m_cur_offset;
  m_cur_offset = new_offset;
  return consumed;
}

