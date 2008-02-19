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

#ifndef HYPERTABLE_APACHELOGPARSER_H
#define HYPERTABLE_APACHELOGPARSER_H

#include <string>

#include <fstream>
#include <iostream>


namespace Hypertable {

  typedef struct {
    char *ip_address;
    char *userid;
    uint64_t timestamp;
    char *request;
    char *response_code;
    char *object_size;
    char *referer;
    char *user_agent;
  } ApacheLogEntryT;

  class ApacheLogParser {

  public:
    ApacheLogParser() : m_last_timestamp(0), m_timestamp_offset(0) { return; }
    void load(std::string filename);
    bool next(ApacheLogEntryT &entry);

  private:

    char *extract_field(char *base, char **field_ptr);
    char *extract_timestamp(char *base, uint64_t *timestampp);

    std::ifstream m_fin;
    std::string m_line;
    uint64_t m_last_timestamp;
    uint32_t m_timestamp_offset;

  };

}

#endif // HYPERTABLE_APACHELOGPARSER_H
