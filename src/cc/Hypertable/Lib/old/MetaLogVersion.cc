/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "MetaLogVersion.h"

namespace Hypertable {

  namespace OldMetaLog {

    using namespace Serialization;

    // range server constants
    const uint16_t RSML_VERSION = 0x0101;
    const char *RSML_PREFIX = "RSML";

    // master constants
    const uint16_t MML_VERSION = 0x0100;
    const char *MML_PREFIX = "MML";

    void MetaLogHeader::encode(uint8_t *buf, size_t len) {
      uint8_t *p = buf;
      size_t prefixlen = strlen(prefix) + 1;

      memcpy(buf, prefix, prefixlen);
      p += prefixlen;
      encode_i16(&p, version);

      HT_EXPECT((int)len == p - buf, Error::FAILED_EXPECTATION);
    }

    void MetaLogHeader::decode(const uint8_t *buf, size_t len) {
      prefix = (const char *) buf;
      size_t prefixlen = strlen(prefix) + 1;

      const uint8_t *p = buf + prefixlen;
      size_t remain = len - prefixlen;

      version = decode_i16(&p, &remain);

      HT_EXPECT(remain == 0, Error::FAILED_EXPECTATION);
    }

    std::ostream &operator<<(std::ostream &out, const MetaLogHeader &h) {
      out << h.prefix <<' '<< ((h.version >> 12) & 0xf) <<'.'
          << ((h.version >> 8) & 0xf) <<'.'<< ((h.version >> 4) & 0xf)
          <<'.'<< (h.version & 0xf);
      return out;
    }

  } // namespace OldMetaLog

} // namespace Hypertable
