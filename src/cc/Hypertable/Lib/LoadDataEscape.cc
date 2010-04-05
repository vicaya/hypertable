/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "LoadDataEscape.h"

using namespace Hypertable;
using namespace std;


bool
LoadDataEscape::escape(const char *in_buf, size_t in_len,
                       const char **out_bufp, size_t *out_lenp) {
  bool transformed = false;
  const char *in_ptr = in_buf;
  const char *in_end = in_buf + in_len;
  const char *last_xfer = in_buf;

  while (in_ptr < in_end) {
    if (*in_ptr == '\n' || *in_ptr == '\t' || *in_ptr == '\\') {
      if (!transformed) {
        m_buf.clear();
        m_buf.reserve((in_len*2)+1);
        transformed = true;
      }
      memcpy(m_buf.ptr, last_xfer, in_ptr-last_xfer);
      m_buf.ptr += (in_ptr-last_xfer);

      *m_buf.ptr++ = '\\';
      if (*in_ptr == '\n')
        *m_buf.ptr++ = 'n';
      else if (*in_ptr == '\t')
        *m_buf.ptr++ = 't';
      else
        *m_buf.ptr++ = '\\';
      in_ptr++;
      last_xfer = in_ptr;
    }
    else
      in_ptr++;
  }

  if (transformed) {
    memcpy(m_buf.ptr, last_xfer, in_ptr-last_xfer);
    m_buf.ptr += (in_ptr-last_xfer);
    *m_buf.ptr = 0;
    *out_bufp = (const char *)m_buf.base;
    *out_lenp = m_buf.fill();
    return true;
  }

  *out_bufp = in_buf;
  *out_lenp = in_len;
  return false;
}


bool
LoadDataEscape::unescape(const char *in_buf, size_t in_len,
                         const char **out_bufp, size_t *out_lenp) {
  bool transformed = false;
  const char *in_ptr = in_buf;
  const char *in_end = in_buf + (in_len - 1);
  const char *last_xfer = in_buf;

  if (in_len < 2) {
    *out_bufp = in_buf;
    *out_lenp = in_len;
    return false;
  }

  while (in_ptr < in_end) {
    if (*in_ptr == '\\' &&
        (*(in_ptr+1) == 'n' || *(in_ptr+1) == 't' || *(in_ptr+1) == '\\')) {
      if (!transformed) {
        m_buf.clear();
        m_buf.reserve(in_len+1);
        transformed = true;
      }
      memcpy(m_buf.ptr, last_xfer, in_ptr-last_xfer);
      m_buf.ptr += (in_ptr-last_xfer);

      if (*(in_ptr+1) == 'n')
        *m_buf.ptr++ = '\n';
      else if (*(in_ptr+1) == 't')
        *m_buf.ptr++ = '\t';
      else
        *m_buf.ptr++ = '\\';
      in_ptr += 2;
      last_xfer = in_ptr;
    }
    else
      in_ptr++;
  }

  in_ptr = in_end + 1;

  if (transformed) {
    memcpy(m_buf.ptr, last_xfer, in_ptr-last_xfer);
    m_buf.ptr += (in_ptr-last_xfer);
    *m_buf.ptr = 0;
    *out_bufp = (const char *)m_buf.base;
    *out_lenp = m_buf.fill();
    return true;
  }

  *out_bufp = in_buf;
  *out_lenp = in_len;
  return false;
}
