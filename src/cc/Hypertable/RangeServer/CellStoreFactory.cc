/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/Filesystem.h"
#include "Common/Serialization.h"

#include <boost/shared_array.hpp>

#include "CellStoreFactory.h"
#include "CellStoreV0.h"
#include "CellStoreV1.h"
#include "CellStoreV2.h"
#include "CellStoreV3.h"
#include "CellStoreV4.h"
#include "CellStoreTrailerV0.h"
#include "CellStoreTrailerV1.h"
#include "CellStoreTrailerV2.h"
#include "CellStoreTrailerV3.h"
#include "CellStoreTrailerV4.h"
#include "Global.h"

using namespace Hypertable;

CellStore *CellStoreFactory::open(const String &name,
                                  const char *start_row, const char *end_row) {
  String start = (start_row) ? start_row : "";
  String end = (end_row) ? end_row : Key::END_ROW_MARKER;
  int64_t file_length;
  int32_t fd;
  size_t nread, amount;
  uint64_t offset;
  uint16_t version;
  uint32_t oflags = 0;

  /** Get the file length **/
  file_length = Global::dfs->length(name);

  if (HT_IO_ALIGNED(file_length))
    oflags = Filesystem::OPEN_FLAG_DIRECTIO;

  /** Open the DFS file **/
  fd = Global::dfs->open(name, oflags);

  amount = (file_length < HT_DIRECT_IO_ALIGNMENT) ? file_length : HT_DIRECT_IO_ALIGNMENT;
  offset = file_length - amount;

  boost::shared_array<uint8_t> trailer_buf( new uint8_t [amount] );

  nread = Global::dfs->pread(fd, trailer_buf.get(), amount, offset);

  if (nread != amount)
    HT_THROWF(Error::DFSBROKER_IO_ERROR,
              "Problem reading trailer for CellStore file '%s'"
              " - only read %d of %lu bytes", name.c_str(),
              (int)nread, (Lu)amount);

  const uint8_t *ptr = trailer_buf.get() + (amount-2);
  size_t remaining = 2;

  version = Serialization::decode_i16(&ptr, &remaining);

  // If file format is < 4 and happens to be aligned, reopen non-directio
  if (version < 4 && oflags) {
    Global::dfs->close(fd);
    fd = Global::dfs->open(name);
  }

  if (version == 4) {
    CellStoreTrailerV4 trailer_v4;
    CellStoreV4 *cellstore_v4;

    if (amount < trailer_v4.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV4 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v4.deserialize(trailer_buf.get() + (amount - trailer_v4.size()));

    cellstore_v4 = new CellStoreV4(Global::dfs.get());
    cellstore_v4->open(name, start, end, fd, file_length, &trailer_v4);
    return cellstore_v4;
  }
  else if (version == 3) {
    CellStoreTrailerV3 trailer_v3;
    CellStoreV3 *cellstore_v3;

    if (amount < trailer_v3.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV3 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v3.deserialize(trailer_buf.get() + (amount - trailer_v3.size()));

    cellstore_v3 = new CellStoreV3(Global::dfs.get());
    cellstore_v3->open(name, start, end, fd, file_length, &trailer_v3);
    return cellstore_v3;
  }
  else if (version == 2) {
    CellStoreTrailerV2 trailer_v2;
    CellStoreV2 *cellstore_v2;

    if (amount < trailer_v2.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV2 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v2.deserialize(trailer_buf.get() + (amount - trailer_v2.size()));

    cellstore_v2 = new CellStoreV2(Global::dfs.get());
    cellstore_v2->open(name, start, end, fd, file_length, &trailer_v2);
    return cellstore_v2;
  }
  else if (version == 1) {
    CellStoreTrailerV1 trailer_v1;
    CellStoreV1 *cellstore_v1;

    if (amount < trailer_v1.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV1 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v1.deserialize(trailer_buf.get() + (amount - trailer_v1.size()));

    cellstore_v1 = new CellStoreV1(Global::dfs.get());
    cellstore_v1->open(name, start, end, fd, file_length, &trailer_v1);
    return cellstore_v1;
  }
  else if (version == 0) {
    CellStoreTrailerV0 trailer_v0;
    CellStoreV0 *cellstore_v0;

    if (amount < trailer_v0.size())
      HT_THROWF(Error::RANGESERVER_CORRUPT_CELLSTORE,
                "Bad length of CellStoreV0 file '%s' - %llu",
                name.c_str(), (Llu)file_length);

    trailer_v0.deserialize(trailer_buf.get() + (amount - trailer_v0.size()));

    cellstore_v0 = new CellStoreV0(Global::dfs.get());
    cellstore_v0->open(name, start, end, fd, file_length, &trailer_v0);
    return cellstore_v0;
  }
  return 0;
}
