/** -*- c++ -*-
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_APACHELOGPARSER_H
#define HYPERTABLE_APACHELOGPARSER_H

#include <string>

#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filtering_stream.hpp>

#include <fstream>
#include <iostream>

namespace Hypertable {

  class ApacheLogEntry {
  public:
    char *ip_address;
    char *userid;
    struct tm tm;
    char *request;
    char *response_code;
    char *object_size;
    char *referer;
    char *user_agent;
  };

  class ApacheLogParser {

  public:
    void load(std::string filename);
    bool next(ApacheLogEntry &entry);

  private:
    char *extract_field(char *base, char **field_ptr);
    char *extract_timestamp(char *base, struct tm *tmp);

    boost::iostreams::filtering_istream m_fin;
    std::string m_line;
  };

}

#endif // HYPERTABLE_APACHELOGPARSER_H
