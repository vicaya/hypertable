/**
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
    bool load(std::string dataDir) {
      struct stat statbuf;
      char *contentData, *wordData, *urlData;
      char *base, *ptr, *last, *str;
      off_t len;
      std::string shakespeareFile = dataDir + "/shakespeare.txt";
      std::string shakespeareFileGz = dataDir + "/shakespeare.txt.gz";
      std::string wordsFile = dataDir + "/words";
      std::string wordsFileGz = dataDir + "/words.gz";
      std::string urlsFile = dataDir + "/urls.txt";
      std::string urlsFileGz = dataDir + "/urls.txt.gz";
      std::string systemCommand;

      /**
       * Load content vector
       */
      if (stat(shakespeareFile.c_str(), &statbuf) != 0) {
	if (stat(shakespeareFileGz.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file 'shakespeare.txt.gz' : %s", strerror(errno));
	  return false;
	}
	systemCommand = "zcat " + shakespeareFileGz + " > " + shakespeareFile;
	if (system(systemCommand.c_str())) {
	  HT_ERRORF("Unable to decompress file '%s'", shakespeareFileGz.c_str());
	  return false;
	}
	if (stat(shakespeareFile.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file '%s' : %s", shakespeareFile.c_str(), strerror(errno));
	  return false;
	}
      }
      if ((contentData = FileUtils::file_to_buffer(shakespeareFile.c_str(), &len)) == 0)
	return false;
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
      if (stat(wordsFile.c_str(), &statbuf) != 0) {
	if (stat(wordsFileGz.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file '%s' : %s", wordsFileGz.c_str(), strerror(errno));
	  return false;
	}
	systemCommand = "zcat " + wordsFileGz + " > " + wordsFile;
	if (system(systemCommand.c_str())) {
	  HT_ERRORF("Unable to decompress file '%s'", wordsFileGz.c_str());
	  return false;
	}
	if (stat(wordsFile.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file '%s' : %s", wordsFile.c_str(), strerror(errno));
	  return false;
	}
      }
      if ((wordData = FileUtils::file_to_buffer(wordsFile.c_str(), &len)) == 0)
	return false;
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
      if (stat(urlsFile.c_str(), &statbuf) != 0) {
	if (stat(urlsFileGz.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file 'urls.txt.gz' : %s", strerror(errno));
	  return false;
	}
	systemCommand = "zcat " + urlsFileGz + " > " + urlsFile;
	if (system(systemCommand.c_str())) {
	  HT_ERRORF("Unable to decompress file '%s'", urlsFileGz.c_str());
	  return false;
	}
	if (stat(urlsFile.c_str(), &statbuf) != 0) {
	  HT_ERRORF("Unable to stat file '%s' : %s", urlsFile.c_str(), strerror(errno));
	  return false;
	}
      }
      if ((urlData = FileUtils::file_to_buffer(urlsFile.c_str(), &len)) == 0)
	return false;
      base = strtok_r(urlData, "\n\r", &last);
      while (base) {
	str = new char [ strlen(base) + 1 ];
	strcpy(str, base);
	urls.push_back(CharPtr(str));
	base = strtok_r(0, "\n\r", &last);
      }

      return true;
    }

    vector<CharPtr> content;
    vector<CharPtr> words;
    vector<CharPtr> urls;
  };

}

#endif // HYPERTABLE_TESTDATA_H
