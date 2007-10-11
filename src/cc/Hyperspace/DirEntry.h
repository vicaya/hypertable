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

#ifndef HYPERSPACE_DIRENTRY_H
#define HYPERSPACE_DIRENTRY_H

#include <string>

namespace Hyperspace {

  /**
   * Listing information for each node within a directory.  A vector
   * of these objects gets passed back to the application via a call to
   * Readdir()
   */
  struct DirEntryT {
    std::string name;
    bool isDirectory;
  };

  struct ltDirEntry {
    bool operator()(const struct DirEntryT &de1, const struct DirEntryT &de2) const {
      if (de1.name == de2.name)
	return (int)de1.isDirectory < (int)de2.isDirectory;
      return de1.name < de2.name;
    }
  };

  size_t EncodedLengthDirEntry(DirEntryT &dirEntry);
  void EncodeDirEntry(uint8_t **bufPtr, DirEntryT &dirEntry);
  bool DecodeRangeDirEntry(uint8_t **bufPtr, size_t *remainingPtr, DirEntryT &dirEntry);
  
}

#endif // HYPERSPACE_DIRENTRY_H
