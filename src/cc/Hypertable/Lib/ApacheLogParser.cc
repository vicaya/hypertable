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
    if ((base = extract_timestamp(base, &entry.tm)) == 0)
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
    if ((ptr = strchr(base, '"')) == 0)
      return 0;
    *ptr++ = 0;
  }
  else if ((ptr = strchr(base, ' ')) != 0)
    *ptr++ = 0;
  else
    ptr += strlen(base);
  if (field_ptr)
    *field_ptr = (*base != 0) ? base : (char *)"-";
  return ptr;
}



/**
 *
 */
char *ApacheLogParser::extract_timestamp(char *base, struct tm *tmp) {
  char *end_ptr;
  char *ptr;

  memset(tmp, 0, sizeof(tm));

  while (isspace(*base))
    base++;
  if (*base != '[')
    return 0;
  base++;
  if ((ptr = strchr(base, ']')) != 0)
    *ptr++ = 0;

  if ((tmp->tm_mday = strtol(base, &end_ptr, 10)) == 0)
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
    tmp->tm_mon = 0;
  else if (!strcasecmp(base, "Feb"))
    tmp->tm_mon = 1;
  else if (!strcasecmp(base, "Mar"))
    tmp->tm_mon = 2;
  else if (!strcasecmp(base, "Apr"))
    tmp->tm_mon = 3;
  else if (!strcasecmp(base, "May"))
    tmp->tm_mon = 4;
  else if (!strcasecmp(base, "Jun"))
    tmp->tm_mon = 5;
  else if (!strcasecmp(base, "Jul"))
    tmp->tm_mon = 6;
  else if (!strcasecmp(base, "Aug"))
    tmp->tm_mon = 7;
  else if (!strcasecmp(base, "Sep"))
    tmp->tm_mon = 8;
  else if (!strcasecmp(base, "Oct"))
    tmp->tm_mon = 9;
  else if (!strcasecmp(base, "Nov"))
    tmp->tm_mon = 10;
  else if (!strcasecmp(base, "Dec"))
    tmp->tm_mon = 11;
  else
    return 0;

  base = end_ptr;
  if ((tmp->tm_year = strtol(base, &end_ptr, 10)) == 0)
    return 0;
  if (*end_ptr != ':')
    return 0;
  tmp->tm_year -= 1900;

  base = end_ptr+1;
  tmp->tm_hour = strtol(base, &end_ptr, 10);
  if (*end_ptr != ':')
    return 0;

  base = end_ptr+1;
  tmp->tm_min = strtol(base, &end_ptr, 10);
  if (*end_ptr != ':')
    return 0;

  base = end_ptr+1;
  tmp->tm_sec = strtol(base, &end_ptr, 10);
  base = end_ptr;

  while (isspace(*base))
    base++;

  long offset = 0;
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

  if (!positive)
    offset *= -1;

  tmp->tm_gmtoff = offset;

  return ptr;
}

