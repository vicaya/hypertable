/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/md5.h"
#include "Common/System.h"

#include "Hypertable/RangeServer/QueryCache.h"

using namespace Hypertable;
using namespace std;


#define MAX_MEMORY 5000000
#define TOTAL_INSERTS 10000
#define TRACK_BUFFER_SIZE 4000

typedef struct {
  QueryCache::Key key;
  char row[3];
} TrackRecT;

int main(int argc, char **argv) {
  QueryCache *cache;
  unsigned long seed = (unsigned long)getpid();
  boost::shared_array<uint8_t> result( new uint8_t [ 1000 ] );
  uint32_t result_length;
  QueryCache::Key key;
  char row[32];
  char keybuf[32];
  TrackRecT  track_buf[TRACK_BUFFER_SIZE];
  size_t track_buf_i = 0;

  System::initialize(System::locate_install_dir(argv[0]));

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--seed=", 7))
      seed = atoi(&argv[i][7]);
  }

  cache = new QueryCache(MAX_MEMORY);

  md5_csum((unsigned char *)"aa", 2, key.digest);

  if (cache->insert(&key, 1, "aa", result, MAX_MEMORY+1)) {
    cout << "Error: insert should have failed." << endl;
    exit(1);
  }

  if (cache->lookup(&key, result, &result_length)) {
    cout << "Error: key should not exist in cache." << endl;
    exit(1);
  }

  for (size_t rowi = (size_t)'a'; rowi <= (size_t)'z'; rowi++) {
    row[0] = (char)rowi;
    row[1] = (char)rowi;
    row[2] = 0;
    for (size_t i=0; i<100; i++) {
      sprintf(keybuf, "%s-%d", row, (int)i);
      md5_csum((unsigned char *)keybuf, strlen(keybuf), key.digest);
      if (!cache->insert(&key, 1, row, result, 1000)) {
	cout << "Error: insert failed." << endl;
	exit(1);
      }
    }
  }

  row[0] = 'a';
  row[1] = 'a';
  row[2] = 0;

  for (size_t i=0; i<100; i++) {
    sprintf(keybuf, "%s-%d", row, (int)i);
    md5_csum((unsigned char *)keybuf, strlen(keybuf), key.digest);
    if (!cache->lookup(&key, result, &result_length)) {
      cout << "Error: key not found." << endl;
      exit(1);
    }
  }

  cache->invalidate(1, row);

  for (size_t i=0; i<100; i++) {
    sprintf(keybuf, "%s-%d", row, (int)i);
    md5_csum((unsigned char *)keybuf, strlen(keybuf), key.digest);
    if (cache->lookup(&key, result, &result_length)) {
      cout << "Error: key found." << endl;
      exit(1);
    }
  }

  for (size_t rowi = (size_t)'b'; rowi <= (size_t)'z'; rowi++) {
    row[0] = (char)rowi;
    row[1] = (char)rowi;
    row[2] = 0;
    cache->invalidate(1, row);
  }

  HT_ASSERT(cache->available_memory() == MAX_MEMORY);

  srandom(1234);

  uint32_t rand_val, charno;

  for (size_t i=0; i<TOTAL_INSERTS; i++) {
    rand_val = (uint32_t)random();
    charno = (random() % 26) + (uint32_t)'a';
    sprintf(keybuf, "%c-%d", charno, (int)rand_val);
    md5_csum((unsigned char *)keybuf, strlen(keybuf), track_buf[track_buf_i].key.digest);
    track_buf[track_buf_i].row[0] = (char)charno;
    track_buf[track_buf_i].row[1] = (char)charno;
    track_buf[track_buf_i].row[2] = 0;
    cache->insert(&track_buf[track_buf_i].key, 1, track_buf[track_buf_i].row, result, 1000);
    track_buf_i = (track_buf_i + 1) % TRACK_BUFFER_SIZE;
  }

  charno = (random() % 26) + (uint32_t)'a';
  row[0] = charno;
  row[1] = charno;
  row[2] = 0;
  cache->invalidate(1, row);

  for (size_t i=0; i<TRACK_BUFFER_SIZE; i++) {
    if (track_buf[i].row[0] == (char)charno)
      HT_ASSERT( !cache->lookup(&track_buf[i].key, result, &result_length) );
    else
      HT_ASSERT( cache->lookup(&track_buf[i].key, result, &result_length) );
  }

  delete cache;

  return 0;
}
