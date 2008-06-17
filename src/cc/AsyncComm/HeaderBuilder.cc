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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "HeaderBuilder.h"

namespace Hypertable {

  void HeaderBuilder::encode(uint8_t **bufp) {
    Header::Common *mheader;
    mheader = (Header::Common *)*bufp;
    mheader->version = Header::VERSION;
    mheader->protocol = m_protocol;
    mheader->flags = m_flags;
    mheader->header_len = sizeof(Header::Common);
    mheader->id = m_id;
    mheader->gid = m_group_id;
    mheader->total_len = m_total_len;
    (*bufp) += sizeof(Header::Common);
  }

}

