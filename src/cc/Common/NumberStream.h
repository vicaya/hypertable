/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_NUMBERSTREAM_H
#define HYPERTABLE_NUMBERSTREAM_H

#include <cstdio>

extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Logger.h"

namespace hypertable {

  class NumberStream {
    
  public:
    NumberStream(const char *fname) {
      struct stat statbuf;
      
      if (stat(fname, &statbuf) != 0) {
	LOG_VA_ERROR("Problem stating file '%s' - %s", fname, strerror(errno));
	exit(1);
      }
      if (statbuf.st_size < (off_t)sizeof(int32_t)) {
	LOG_VA_ERROR("Number stream file '%s' is not big enough, must be at least 4 bytes long", fname);
	exit(1);
      }
      
      if ((mFp = fopen(fname, "r")) == 0) {
	LOG_VA_ERROR("Unable to open number stream file '%s'", fname);
	exit(1);
      }
    }
    ~NumberStream() {
      fclose(mFp);
    }
    uint32_t getInt() {
      uint32_t number;

      if (fread(&number, sizeof(int32_t), 1, mFp) == 0) {
	fseek(mFp, 0L, SEEK_SET);
	if (fread(&number, sizeof(int32_t), 1, mFp) == 0) {
	  LOG_ERROR("Problem reading integer from number stream, exiting...");
	  exit(1);
	}
      }

      return number;
    }

  private:
    FILE *mFp;
  };

}

#endif // HYPERTABLE_NUMBERSTREAM_H
