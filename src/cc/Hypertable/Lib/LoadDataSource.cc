/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <cerrno>
#include <cstring>

#include <boost/algorithm/string.hpp>
#include <boost/shared_array.hpp>

extern "C" {
#include <strings.h>
}

#include "Common/ByteOrder.h"
#include "Common/DynamicBuffer.h"

#include "Key.h"

#include "LoadDataSource.h"

using namespace std;

/**
 * 
 */
bool LoadDataSource::next(uint64_t *timestampp, KeySpec *keyp, uint8_t **valuep, uint32_t *value_lenp) {
  string line;
  char *base, *ptr, *colon;

  while (getline(m_fin, line)) {
    m_cur_line++;

    m_line_buffer.clear();

    m_line_buffer.add(line.c_str(), strlen(line.c_str())+1);

    /**
     *  Get timestamp
     */
    base = (char *)m_line_buffer.buf;
    if ((ptr = strchr(base, '\t')) == 0) {
      cerr << "error: too few fields on line " << (m_cur_line-1) << endl;
      continue;
    }
    *ptr++ = 0;

    if (!strcmp(base, "AUTO"))
      *timestampp = 0LL;
    else {
      *timestampp = strtoll(base, 0, 0);
      if (*timestampp == 0 && errno == EINVAL) {
	cerr << "error: invalid timestamp (" << base << ") on line " << (m_cur_line-1) << endl;
	continue;
      }
    }

    /**
     * Get row key
     */
    base = ptr;
    if ((ptr = strchr(base, '\t')) == 0) {
      cerr << "error: too few fields on line " << (m_cur_line-1) << endl;
      continue;
    }
    keyp->row = base;
    keyp->row_len = ptr - base;
    *ptr++ = 0;

    /**
     * Get column family and qualifier
     */
    base = ptr;
    if ((ptr = strchr(base, '\t')) == 0) {
      cerr << "error: too few fields on line " << (m_cur_line-1) << endl;
      continue;
    }
    *ptr = 0;

    if ((colon = strchr(base, ':')) != 0) {
      *colon++ = 0;
      keyp->column_family = base;
      if (colon < ptr) {
	keyp->column_qualifier = colon;
	keyp->column_qualifier_len = ptr - colon;
      }
      else {
	keyp->column_qualifier = 0;
	keyp->column_qualifier_len = 0;
      }
    }
    else {
      keyp->column_qualifier = 0;
      keyp->column_qualifier_len = 0;
    }
    keyp->column_family = base;
    ptr++;

    /**
     * Get value
     */
    *valuep = (uint8_t *)ptr;
    *value_lenp = strlen(ptr);

    return true;
  }

  return false;
}
