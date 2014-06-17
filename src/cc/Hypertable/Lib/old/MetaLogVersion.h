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

#ifndef HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H
#define HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H

#include <iostream>

namespace Hypertable {

  namespace OldMetaLog {

    // sizes
    enum {
      ML_ENTRY_HEADER_SIZE = /*checksum*/4 + /*ts*/ 8 + /*type*/1 + /*payload*/4,
      RSML_HEADER_SIZE = /*prefix*/5 + /*version*/2,
      MML_HEADER_SIZE = /*prefix*/4 + /*version*/2
    };

    // range server constants
    extern const uint16_t RSML_VERSION;
    extern const char *RSML_PREFIX;

    // master constants
    extern const uint16_t MML_VERSION;
    extern const char *MML_PREFIX;

    struct MetaLogHeader {
      MetaLogHeader(const char *prfx, uint16_t vernum)
        : prefix(prfx), version(vernum) {}
      MetaLogHeader(const uint8_t *buf, size_t len) { decode(buf, len); }

      void encode(uint8_t *buf, size_t len);
      void decode(const uint8_t *buf, size_t len);

      const char *prefix;
      uint16_t version;
    };

    std::ostream &operator<<(std::ostream &, const MetaLogHeader &);

  } // namespace OldMetaLog

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_SERVER_METALOG_VERSION_H


