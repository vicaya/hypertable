/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "FileUtils.h"
#include "System.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage_str = 
  "\n" \
  "usage: code_search_and_replace [OPTIONS] <file> [ <file> ...]\n" \
  "\n" \
  "OPTIONS";

  struct lt_decreasing_length {
    bool operator()(const pair<string, string> &m1, const pair<string, string> &m2) const {
      if (m1.first == m2.first) {
	cout << "error: multiple mappings for '" << m1.first << "'" << endl;
	exit(1);
      }
      return m1.first.length() >= m2.first.length();
    }
  };

  bool convert_file(string input_filename, string license_filename, vector< pair<string, string> > &exchange_strings, string &converted_contents);
  
}



/**
 * 
 */
int main(int argc, char **argv) {
  vector< pair<string, string> > exchange_strings;
  string converted_contents;
  string orig_name;
  string backup_name;
  string license_name;
  char *ptr;

  System::initialize(argv[0]);

  try {

    po::options_description generic(usage_str);
    generic.add_options()
      ("help", "Display this help message")
      ("map-file", po::value<string>(), "File containing lines of string mappings of the form:  <from> '\\t' <to>")
      ("mapping", po::value< vector<string> >(), "String mapping.  Example: --mapping=\"foo\\tbar\"")
      ("license-file", po::value<string>(), "File containing source code header comment")
      ;

    // Hidden options: server location
    po::options_description hidden("Hidden options");
    hidden.add_options()
      ("input-file", po::value< vector<string> >(), "input file")
      ;

    po::options_description cmdline_options;
    cmdline_options.add(generic).add(hidden);

    po::positional_options_description p;
    p.add("input-file", -1);

    po::variables_map vm;
    store(po::command_line_parser(argc, argv).
	  options(cmdline_options).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help") || vm.count("input-file") == 0) {
      cout << generic << "\n";
      return 1;
    }

    if (vm.count("map-file") == 0 && vm.count("license-file") == 0 && vm.count("mapping") == 0) {
      cout << "error: --map-file, --mapping, and/or --license-file must be supplied." << endl;
      return 1;
    }

    vector<string> input_file, mapping;

    input_file = vm["input-file"].as< vector<string> >();

    if (vm.count("mapping"))
      mapping = vm["mapping"].as< vector<string> >();

    if (vm.count("license-file"))
      license_name = vm["license-file"].as<string>();

    for (size_t i=0; i<mapping.size(); i++) {
      if ((ptr = strstr(mapping[i].c_str(), "\\t")) == 0) {
	cout << "error: bad mapping - " << mapping[i] << endl;
	return 1;
      }
      *ptr = 0;
      ptr += 2;
      exchange_strings.push_back( make_pair(mapping[i].c_str(), ptr) );
    }

    if (vm.count("map-file")) {
      std::ifstream mappings_in( vm["map-file"].as< string >().c_str() );
      std::string line;

      while (getline(mappings_in, line)) {
	boost::trim(line);
	if (line != "") {
	  char *line_ptr = (char *)line.c_str();
	  if ((ptr = strstr(line_ptr, "\t")) == 0) {
	    cout << "error: bad mapping - " << line << endl;
	    return 1;
	  }
	  *ptr++ = 0;
	  exchange_strings.push_back( make_pair(line_ptr, ptr) );
	}
      }
    }

    {
      struct lt_decreasing_length sort_obj;
      sort(exchange_strings.begin(), exchange_strings.end(), sort_obj);
    }

    for (size_t i=0; i<input_file.size(); i++) {
      converted_contents = "";
      cout << "Converting '" << input_file[i] << "'" << endl << flush;
      if (!convert_file(input_file[i], license_name, exchange_strings, converted_contents))
	cerr << "Problem converting '" << input_file[i] << "'" << endl;
      else {
	orig_name = input_file[i];
	backup_name = input_file[i] + ".old";
	unlink(backup_name.c_str());
	rename(orig_name.c_str(), backup_name.c_str());
	{
	  ofstream fout(input_file[i].c_str());
	  fout << converted_contents;
	  fout.close();
	}
      }
    }

  }
  catch(exception& e) {
    cerr << "error: " << e.what() << "\n";
  }
  catch(...) {
    cerr << "Exception of unknown type!\n";
  }

}

namespace {

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

  bool convert_file(string input_filename, string license_filename, vector< pair<string, string> > &exchange_strings, string &converted_contents) {
    off_t lastOffset, flen, license_len;
    char *fcontents = FileUtils::file_to_buffer(input_filename.c_str(), &flen);
    char *ptr = fcontents;
    char *codeStart, *base;
    ReplaceInfoT rinfo;
    vector<ReplaceInfoT> riVec;

    if (fcontents == 0)
      return false;

    if (license_filename != "") {
      
      converted_contents = FileUtils::file_to_buffer(license_filename.c_str(), &license_len);

      boost::trim(converted_contents);
      converted_contents += "\n\n";

      while (true) {

	while (*ptr && isspace(*ptr))
	  ptr++;

	if (strncmp(ptr, "/*", 2)) {
	  cerr << "Comment header not found in '" << input_filename << "'" << endl;
	  return false;
	}

	if ((ptr = strstr(ptr, "*/")) == 0) {
	  cerr << "Unable to find end of comment header in '" << input_filename << "'" << endl;
	  return false;
	}

	ptr += 2;

	while (*ptr && isspace(*ptr))
	  ptr++;

	if (strncmp(ptr, "/*", 2))
	  break;
      }

    }

    codeStart = ptr;

    for (size_t i=0; i<exchange_strings.size(); i++) {
      base = codeStart;
      while ((ptr = strstr(base, exchange_strings[i].first.c_str())) != 0) {
	rinfo.offset = ptr - fcontents;
	rinfo.fromString = exchange_strings[i].first.c_str();
	rinfo.toString = exchange_strings[i].second.c_str();
	riVec.push_back(rinfo);
	base = ptr + strlen(exchange_strings[i].first.c_str());
      }
    }

    if (!riVec.empty()) {
      ltReplaceInfo ltObj;      
      sort(riVec.begin(), riVec.end(), ltObj);
    }

    lastOffset = codeStart - fcontents;
    for (size_t i=0; i<riVec.size(); i++) {
      cerr << "Replace at offset " << riVec[i].offset << endl << flush;
      converted_contents += string(fcontents + lastOffset, riVec[i].offset-lastOffset);
      converted_contents += riVec[i].toString;
      lastOffset = riVec[i].offset + strlen(riVec[i].fromString);
    }

    converted_contents += string(fcontents + lastOffset, flen-lastOffset);

    return true;
  }

}


