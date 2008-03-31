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

#include "ApacheLogParser.h"

extern "C" {
#include <limits.h>
#include <stdlib.h>
#include <time.h>
}

using namespace Hypertable;



void ApacheLogParser::load(std::string filename) {
  m_fin.open(filename.c_str());
}



/**
 */
bool ApacheLogParser::next(ApacheLogEntryT &entry) {
  uint64_t timestamp;
  char *base;

  while (true) {

    memset(&entry, 0, sizeof(entry));

    if (!getline(m_fin, m_line))
      return false;

    base = (char *)m_line.c_str();

    // IP address
    if ((base = extract_field(base, &entry.ip_address)) == 0)
      continue;

    // skip identd
    if ((base = extract_field(base, 0)) == 0)
      continue;

    // userid
    if ((base = extract_field(base, &entry.userid)) == 0)
      continue;

    // timestamp
    if ((base = extract_timestamp(base, &timestamp)) == 0)
      continue;

    // make sure there are no timestamp collisions
    if (timestamp == m_last_timestamp)
      m_timestamp_offset++;
    else {
      m_timestamp_offset=0;
      m_last_timestamp = timestamp;
    }
    entry.timestamp = timestamp + m_timestamp_offset;

    if (entry.timestamp == 0)
      continue;

    // request
    if ((base = extract_field(base, &entry.request)) == 0)
      continue;

    // response_code
    if ((base = extract_field(base, &entry.response_code)) == 0)
      continue;

    // object_size
    if ((base = extract_field(base, &entry.object_size)) == 0)
      continue;

    // referer
    if ((base = extract_field(base, &entry.referer)) == 0)
      return true;

    // user_agent
    if ((base = extract_field(base, &entry.user_agent)) == 0)
      return true;

    return true;
  }

}



/**
 *
 */
char *ApacheLogParser::extract_field(char *base, char **field_ptr) {
  char *ptr;
  while (isspace(*base))
    base++;
  if (*base == '"') {
    base++;
    if ((ptr = strchr(base, '"')) != 0)
      *ptr++ = 0;
  }
  else if ((ptr = strchr(base, ' ')) != 0)
    *ptr++ = 0;
  if (field_ptr) {
    if (*base == 0 || (*base == '-' && *(base+1) == 0))
      *field_ptr = 0;
    else
      *field_ptr = base;
  }
  return ptr;
}



/**
 *
 */
char *ApacheLogParser::extract_timestamp(char *base, uint64_t *timestampp) {
  struct tm tm;
  char *end_ptr;
  char *ptr;

  memset(&tm, 0, sizeof(tm));

  *timestampp = 0;

  while (isspace(*base))
    base++;
  if (*base != '[')
    return 0;
  base++;
  if ((ptr = strchr(base, ']')) != 0)
    *ptr++ = 0;

  if ((tm.tm_mday = strtol(base, &end_ptr, 10)) == 0)
    return 0;
  if (*end_ptr != '/')
    return 0;
  base = end_ptr+1;

  end_ptr = (char *)base;
  while (isalpha(*end_ptr))
    end_ptr++;
  if (*end_ptr != '/' || (end_ptr-base) != 3)
    return 0;
  *end_ptr++ = 0;
  if (!strcasecmp(base, "Jan"))
    tm.tm_mon = 0;
  else if (!strcasecmp(base, "Feb"))
    tm.tm_mon = 1;
  else if (!strcasecmp(base, "Mar"))
    tm.tm_mon = 2;
  else if (!strcasecmp(base, "Apr"))
    tm.tm_mon = 3;
  else if (!strcasecmp(base, "May"))
    tm.tm_mon = 4;
  else if (!strcasecmp(base, "Jun"))
    tm.tm_mon = 5;
  else if (!strcasecmp(base, "Jul"))
    tm.tm_mon = 6;
  else if (!strcasecmp(base, "Aug"))
    tm.tm_mon = 7;
  else if (!strcasecmp(base, "Sep"))
    tm.tm_mon = 8;
  else if (!strcasecmp(base, "Oct"))
    tm.tm_mon = 9;
  else if (!strcasecmp(base, "Nov"))
    tm.tm_mon = 10;
  else if (!strcasecmp(base, "Dec"))
    tm.tm_mon = 11;
  else
    return 0;

  base = end_ptr;
  if ((tm.tm_year = strtol(base, &end_ptr, 10)) == 0)
    return 0;
  if (*end_ptr != ':')
    return 0;
  tm.tm_year -= 1900;

  base = end_ptr+1;
  tm.tm_hour = strtol(base, &end_ptr, 10);
  if (*end_ptr != ':')
    return 0;

  base = end_ptr+1;
  tm.tm_min = strtol(base, &end_ptr, 10);
  if (*end_ptr != ':')
    return 0;

  base = end_ptr+1;
  tm.tm_sec = strtol(base, &end_ptr, 10);
  base = end_ptr;

  while (isspace(*base))
    base++;

  uint64_t offset = 0;
  bool positive = true;
  if (*base) {
    if (*base == '+')
      positive = true;
    else if (*base == '-')
      positive = false;
    else
      return 0;
    base++;

    if (!isdigit(*base))
      return 0;
    offset += 360000 * (*base-'0');
    if (*++base == 0)
      return 0;

    if (!isdigit(*base))
      return 0;
    offset += 36000 * (*base-'0');
    if (*++base == 0)
      return 0;

    if (!isdigit(*base))
      return 0;
    offset += 600 * (*base-'0');
    if (*++base == 0)
      return 0;

    if (!isdigit(*base))
      return 0;
    offset += 60 * (*base-'0');
  }

  if ((*timestampp = timegm(&tm)) == (uint64_t)-1)
    return 0;

  if (!positive)
    *timestampp -= offset;
  else
    *timestampp += offset;

  *timestampp *= 1000000000LL;

  return ptr;
}

