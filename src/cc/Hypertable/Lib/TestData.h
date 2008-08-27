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

#ifndef HYPERTABLE_TESTDATA_H
#define HYPERTABLE_TESTDATA_H

#include <boost/shared_array.hpp>

#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/TestHarness.h"

namespace Hypertable {

  typedef boost::shared_array<const char> CharPtr;

  class TestData {
  public:
    bool load(std::string datadir) {
      struct stat statbuf;
      char *contentdata, *worddata, *urldata;
      char *base, *ptr, *last, *str;
      off_t len;
      std::string shakespearefile = datadir + "/shakespeare.txt";
      std::string shakespearegz = datadir + "/shakespeare.txt.gz";
      std::string wordsfile = datadir + "/words";
      std::string wordsgz = datadir + "/words.gz";
      std::string urlsfile = datadir + "/urls.txt";
      std::string urlsgz = datadir + "/urls.txt.gz";
      std::string syscmd;

      /**
       * Load content vector
       */
      if (stat(shakespearefile.c_str(), &statbuf) != 0) {
        if (stat(shakespearegz.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file 'shakespeare.txt.gz' : %s", strerror(errno));
          return false;
        }
        syscmd = "zcat " + shakespearegz + " > " + shakespearefile;
        if (system(syscmd.c_str())) {
          HT_ERRORF("Unable to decompress file '%s'", shakespearegz.c_str());
          return false;
        }
        if (stat(shakespearefile.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file '%s' : %s", shakespearefile.c_str(), strerror(errno));
          return false;
        }
      }
      if ((contentdata = FileUtils::file_to_buffer(shakespearefile.c_str(), &len)) == 0)
        return false;
      base = contentdata;
      while ((ptr = strstr(base, "\n\n")) != 0) {
        *ptr++ = 0;
        str = new char [strlen(base) + 1];
        strcpy(str, base);
        content.push_back(CharPtr(str));
        base = ptr + 2;
      }

      delete [] contentdata;

      /**
       * Load words vector
       */
      if (stat(wordsfile.c_str(), &statbuf) != 0) {
        if (stat(wordsgz.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file '%s' : %s", wordsgz.c_str(), strerror(errno));
          return false;
        }
        syscmd = "zcat " + wordsgz + " > " + wordsfile;
        if (system(syscmd.c_str())) {
          HT_ERRORF("Unable to decompress file '%s'", wordsgz.c_str());
          return false;
        }
        if (stat(wordsfile.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file '%s' : %s", wordsfile.c_str(), strerror(errno));
          return false;
        }
      }
      if ((worddata = FileUtils::file_to_buffer(wordsfile.c_str(), &len)) == 0)
        return false;
      base = strtok_r(worddata, "\n\r", &last);
      while (base) {
        str = new char [strlen(base) + 1];
        strcpy(str, base);
        words.push_back(CharPtr(str));
        base = strtok_r(0, "\n\r", &last);
      }
      delete [] worddata;


      /**
       * Load urls vector
       */
      if (stat(urlsfile.c_str(), &statbuf) != 0) {
        if (stat(urlsgz.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file 'urls.txt.gz' : %s", strerror(errno));
          return false;
        }
        syscmd = "zcat " + urlsgz + " > " + urlsfile;
        if (system(syscmd.c_str())) {
          HT_ERRORF("Unable to decompress file '%s'", urlsgz.c_str());
          return false;
        }
        if (stat(urlsfile.c_str(), &statbuf) != 0) {
          HT_ERRORF("Unable to stat file '%s' : %s", urlsfile.c_str(), strerror(errno));
          return false;
        }
      }
      if ((urldata = FileUtils::file_to_buffer(urlsfile.c_str(), &len)) == 0)
        return false;
      base = strtok_r(urldata, "\n\r", &last);
      while (base) {
        str = new char [strlen(base) + 1];
        strcpy(str, base);
        urls.push_back(CharPtr(str));
        base = strtok_r(0, "\n\r", &last);
      }

      return true;
    }

    std::vector<CharPtr> content;
    std::vector<CharPtr> words;
    std::vector<CharPtr> urls;
  };

}

#endif // HYPERTABLE_TESTDATA_H
