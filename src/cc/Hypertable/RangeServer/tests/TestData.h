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


#include <boost/shared_array.hpp>

#include "Common/TestHarness.h"

typedef boost::shared_array<const char> CharPtr;

class TestData {
 public:
  TestData(TestHarness &harness) {
    struct stat statbuf;
    char *contentData, *wordData, *urlData;
    char *base, *ptr, *last, *str;
    off_t len;
    
    /**
     * Load content vector
     */
    if (stat("shakespeare.txt", &statbuf) != 0) {
      if (stat("shakespeare.txt.gz", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'shakespeare.txt.gz' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
      if (system("gunzip shakespeare.txt.gz")) {
	LOG_ERROR("Unable to decompress file 'shakespeare.txt.gz'");
	harness.DisplayErrorAndExit();
      }
      if (stat("shakespeare.txt", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'shakespeare.txt' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
    }
    if ((contentData = FileUtils::FileToBuffer("shakespeare.txt", &len)) == 0)
      harness.DisplayErrorAndExit();
    base = contentData;
    while ((ptr = strstr(base, "\n\n")) != 0) {
      *ptr++ = 0;
      str = new char [ strlen(base) + 1 ];
      strcpy(str, base);
      content.push_back(CharPtr(str));
      base = ptr + 2;
    }

    delete [] contentData;

    /**
     * Load words vector
     */
    if (stat("words", &statbuf) != 0) {
      if (stat("words.gz", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'words.gz' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
      if (system("gunzip words.gz")) {
	LOG_ERROR("Unable to decompress file 'words.gz'");
	harness.DisplayErrorAndExit();
      }
      if (stat("words", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'words' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
    }
    if ((wordData = FileUtils::FileToBuffer("words", &len)) == 0)
      harness.DisplayErrorAndExit();
    base = strtok_r(wordData, "\n\r", &last);
    while (base) {
      str = new char [ strlen(base) + 1 ];
      strcpy(str, base);
      words.push_back(CharPtr(str));
      base = strtok_r(0, "\n\r", &last);
    }
    delete [] wordData;


    /**
     * Load urls vector
     */
    if (stat("urls.txt", &statbuf) != 0) {
      if (stat("urls.txt.gz", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'urls.txt.gz' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
      if (system("gunzip urls.txt.gz")) {
	LOG_ERROR("Unable to decompress file 'urls.txt.gz'");
	harness.DisplayErrorAndExit();
      }
      if (stat("urls.txt", &statbuf) != 0) {
	LOG_VA_ERROR("Unable to stat file 'urls.txt' : %s", strerror(errno));
	harness.DisplayErrorAndExit();
      }
    }
    if ((urlData = FileUtils::FileToBuffer("urls.txt", &len)) == 0)
      harness.DisplayErrorAndExit();
    base = strtok_r(urlData, "\n\r", &last);
    while (base) {
      str = new char [ strlen(base) + 1 ];
      strcpy(str, base);
      urls.push_back(CharPtr(str));
      base = strtok_r(0, "\n\r", &last);
    }

  }

  vector<CharPtr> content;
  vector<CharPtr> words;
  vector<CharPtr> urls;

};


