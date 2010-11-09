/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <cstdlib>
#include <cstdio>
#include <iostream>
#include <list>
#include <vector>

extern "C" {
#include <limits.h>
#include <sys/types.h>
#include <unistd.h>

}

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "Hypertable/RangeServer/FileBlockCache.h"

#ifdef _WIN32
#define random rand
#define srandom srand
#endif

using namespace Hypertable;
using namespace std;

namespace {
  struct BufferRecord {
    int file_id;
    uint32_t file_offset;
    uint32_t length;
  };
  struct LtBufferRecord {
    bool operator()(const struct BufferRecord &br1,
                    const struct BufferRecord &br2) const {
      if (br1.file_id == br2.file_id) {
        if (br1.file_offset == br2.file_offset)
          return br1.length < br2.length;
        return br1.file_offset < br2.file_offset;
      }
      return br1.file_id < br2.file_id;
    }
  };
}

#define TOTAL_ALLOC_LIMIT 100000000
#define TARGET_BUFSIZE (2 * 65536)
#define MAX_FILE_ID 10
#define MAX_FILE_OFFSET 100

int main(int argc, char **argv) {
  FileBlockCache *cache;
  vector<BufferRecord> input_data;
  list<BufferRecord> history;
  BufferRecord rec;
  unsigned long seed = (unsigned long)getpid();
  uint64_t total_alloc = 0;
  uint64_t total_memory = TOTAL_ALLOC_LIMIT;  
  int file_id;
  uint32_t file_offset;
  uint8_t *block;
  uint32_t length;
  int index;
  set<BufferRecord, LtBufferRecord> lru;

  System::initialize(System::locate_install_dir(argv[0]));

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--seed=", 7))
      seed = atoi(&argv[i][7]);
    else if (!strncmp(argv[i], "--total-memory=", 15))
      total_memory = std::max(strtoll(&argv[i][15], 0, 0), (int64_t)(TARGET_BUFSIZE * 2));
  }
  uint64_t cache_memory = total_memory / 2;
  cache = new FileBlockCache(cache_memory, cache_memory);

  srandom(seed);

  input_data.reserve(MAX_FILE_ID*MAX_FILE_OFFSET);
  for (int i=0; i<MAX_FILE_ID; i++) {
    for (int j=0; j<MAX_FILE_OFFSET; j++) {
      rec.file_id = i;
      rec.file_offset = j;
      rec.length = (uint32_t)(random() % TARGET_BUFSIZE);
      input_data.push_back(rec);
    }
  }

  cout << "FileBlockCache_test SEED = " << seed << ", total-memory = " << total_memory << endl;

  /**
   * Check to make sure cache rejects items that are too large
   */
  if (cache->insert_and_checkout(0, 0, 0, total_memory+1)) {
    HT_ERROR("Cache accepted too large of an item");
    return 1;
  }

  while (total_alloc < total_memory) {
    index = (int)(random() % (MAX_FILE_ID*MAX_FILE_OFFSET));
    file_id = input_data[index].file_id;
    file_offset = input_data[index].file_offset;
    if (cache->checkout(file_id, file_offset, &block, &length))
      cache->checkin(file_id, file_offset);
    else {
      length = input_data[index].length;
      block = new uint8_t [ length ];
      HT_EXPECT(cache->insert_and_checkout(file_id, file_offset, block, length),
                Error::FAILED_EXPECTATION);
      total_alloc += length;
      cache->checkin(file_id, file_offset);
    }
    history.push_front(input_data[index]);
  }

  /**
   * Now verify that our idea of LRU is actually in the cache
   */
  total_alloc = 0;
  list<BufferRecord>::iterator history_iter = history.begin();
  while (total_alloc < total_memory) {
    rec.file_id = (*history_iter).file_id;
    rec.file_offset = (*history_iter).file_offset;
    rec.length = (*history_iter).length;
    if (lru.find(rec) == lru.end()) {
      if (total_alloc + rec.length > cache_memory)
        break;
      lru.insert(rec);
      total_alloc += rec.length;
      if (!cache->contains(rec.file_id, rec.file_offset)) {
        HT_ERRORF("computed LRU does not match cache's (id=%d, offset=%u)",
                  rec.file_id, rec.file_offset);
        return 1;
      }
    }
    ++history_iter;
  }

  /**
   * Now verify that all of the remaining items that
   * should *not* be in our LRU are, indeed, not in the cache
   */
  if (cache->contains(rec.file_id, rec.file_offset)) {
    HT_ERROR("computed LRU does not match cache's");
    return 1;
  }
  while (history_iter != history.end()) {
    rec.file_id = (*history_iter).file_id;
    rec.file_offset = (*history_iter).file_offset;
    rec.length = (*history_iter).length;
    if (lru.find(rec) == lru.end()) {
      if (cache->contains(rec.file_id, rec.file_offset)) {
        HT_ERROR("computed LRU does not match cache's");
        return 1;
      }
    }
    ++history_iter;
  }

  delete cache;

  return 0;
}
