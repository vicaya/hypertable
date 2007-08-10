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

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

extern "C" {
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
}

#include "FileUtils.h"

using namespace hypertable;
using namespace std;

namespace {
  bool ConvertFile(const char *fname, string &convertedContents);
}



/**
 * 
 */
int main(int argc, char **argv) {
  string convertedContents;
  string origName;
  string backupName;

  for (int i=1; i<argc; i++) {
    cout << "Converting '" << argv[i] << "'" << endl << flush;
    if (!ConvertFile(argv[i], convertedContents))
      cerr << "Problem converting '" << argv[i] << "'" << endl;
    else {
      origName = argv[i];
      backupName = (string)argv[i] + ".old";
      unlink(backupName.c_str());
      rename(origName.c_str(), backupName.c_str());
      {
	ofstream fout(argv[i]);
	fout << convertedContents;
	fout.close();
      }
    }
  }

}

namespace {

  pair<const char *, const char *> exchangeStrings[] = {
#if 0
    std::make_pair("com.zvents.placer", "org.hypertable"),
    std::make_pair("TABLETSERVER", "RANGESERVER"),
    std::make_pair("PLACER", "HYPERTABLE"),
    std::make_pair("Bigtable", "Hypertable"),
    std::make_pair("namespace Placer", "namespace hypertable"),
    std::make_pair("namespace Bigtable", "namespace hypertable"),
    std::make_pair("namespace Hdfs", "namespace hypertable"),
    std::make_pair("Bigtable/", "Hypertable/"),
    std::make_pair("BIGTABLE", "HYPERTABLE"),
    std::make_pair("Bigtable::", "hypertable::"),
    std::make_pair("Placer::", "hypertable::"),
    std::make_pair("Placerfs::", "hypertable::"),
    std::make_pair("Memtable", "CellCache"),
    std::make_pair("TabletServer", "RangeServer"),
    std::make_pair("SSTable", "CellStore"),
    std::make_pair("SSTABLE", "CELLSTORE"),
    std::make_pair("Version0", "V0"),
    std::make_pair("Version1", "V1"),
    std::make_pair("LocalityGroup", "AccessGroup"),
    std::make_pair("PersistentTable", "CellStore"),
    std::make_pair("TabletInfo", "RangeInfo"),
    std::make_pair("TableScanner", "CellSequenceScanner"),
    std::make_pair((const char *)0, (const char *)0)
#endif
  };

  typedef struct {
    off_t offset;
    const char *fromString;
    const char *toString;
  } ReplaceInfoT;

  struct ltReplaceInfo {
    bool operator()(const ReplaceInfoT &ri1, const ReplaceInfoT &ri2) const {
      return ri1.offset < ri2.offset;
    }
  };

  const char *commentHeader = \
  "/**\n"
  " * Copyright (C) 2007 Doug Judd (Zvents, Inc.)\n"
  " * \n"
  " * This file is part of Hypertable.\n"
  " * \n"
  " * Hypertable is free software; you can redistribute it and/or\n"
  " * modify it under the terms of the GNU General Public License\n"
  " * as published by the Free Software Foundation; either version 2\n"
  " * of the License, or any later version.\n"
  " * \n"
  " * Hypertable is distributed in the hope that it will be useful,\n"
  " * but WITHOUT ANY WARRANTY; without even the implied warranty of\n"
  " * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the\n"
  " * GNU General Public License for more details.\n"
  " * \n"
  " * You should have received a copy of the GNU General Public License\n"
  " * along with this program; if not, write to the Free Software\n"
  " * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.\n"
  " */";

  bool ConvertFile(const char *fname, string &convertedContents) {
    off_t lastOffset, flen;
    char  *fcontents = FileUtils::FileToBuffer(fname, &flen);
    char *ptr = fcontents;
    char *codeStart, *base;
    ReplaceInfoT rinfo;
    vector<ReplaceInfoT> riVec;

    convertedContents = commentHeader;

    if (fcontents == 0)
      return false;

    while (*ptr && isspace(*ptr))
      ptr++;

    if (strncmp(ptr, "/*", 2)) {
      cerr << "Comment header not found in '" << fname << "'" << endl;
      return false;
    }

    if ((ptr = strstr(ptr, "*/")) == 0) {
      cerr << "Unable to find end of comment header in '" << fname << "'" << endl;
      return false;
    }

    codeStart = ptr + 2;

    for (size_t i=0; exchangeStrings[i].first != 0; i++) {
      base = codeStart;
      while ((ptr = strstr(base, exchangeStrings[i].first)) != 0) {
	rinfo.offset = ptr - fcontents;
	rinfo.fromString = exchangeStrings[i].first;
	rinfo.toString = exchangeStrings[i].second;
	riVec.push_back(rinfo);
	base = ptr + strlen(exchangeStrings[i].first);
      }
    }

    if (!riVec.empty()) {
      ltReplaceInfo ltObj;      
      sort(riVec.begin(), riVec.end(), ltObj);
    }

    lastOffset = codeStart - fcontents;
    for (size_t i=0; i<riVec.size(); i++) {
      cerr << "Replace at offset " << riVec[i].offset << endl << flush;
      convertedContents += string(fcontents + lastOffset, riVec[i].offset-lastOffset);
      convertedContents += riVec[i].toString;
      lastOffset = riVec[i].offset + strlen(riVec[i].fromString);
    }

    convertedContents += string(fcontents + lastOffset, flen-lastOffset);

    return true;
  }

}


