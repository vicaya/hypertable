/* 
   Unix SMB/Netbios implementation.
   Version 1.9.
   replacement routines for broken systems
   Copyright (C) Andrew Tridgell 1992-1997
   
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#include "mktime_replacement.h"

using namespace Hypertable;

/*******************************************************************
a mktime() replacement for those who don't have it - contributed by 
C.A. Lademann <cal@zls.com>
********************************************************************/
#define  MINUTE  60
#define  HOUR    60*MINUTE
#define  DAY     24*HOUR
#define  YEAR    365*DAY
time_t Hypertable::mktime_replacement(struct tm *t) {
  struct tm       *u;
  time_t  epoch = 0;
  int     mon [] = { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
    y, m, i;

    if(t->tm_year < 70)
      return((time_t)-1);

    epoch = (t->tm_year - 70) * YEAR + 
      (t->tm_year / 4 - 70 / 4 - t->tm_year / 100) * DAY;

    y = t->tm_year;
    m = 0;

    for(i = 0; i < t->tm_mon; i++) {
      epoch += mon [m] * DAY;
      if(m == 1 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0))
	epoch += DAY;
    
      if(++m > 11) {
	m = 0;
	y++;
      }
    }

    epoch += (t->tm_mday - 1) * DAY;
    epoch += t->tm_hour * HOUR + t->tm_min * MINUTE + t->tm_sec;
  
    if((u = localtime(&epoch)) != NULL) {
      t->tm_sec = u->tm_sec;
      t->tm_min = u->tm_min;
      t->tm_hour = u->tm_hour;
      t->tm_mday = u->tm_mday;
      t->tm_mon = u->tm_mon;
      t->tm_year = u->tm_year;
      t->tm_wday = u->tm_wday;
      t->tm_yday = u->tm_yday;
      t->tm_isdst = u->tm_isdst;
    }

    return(epoch);
}
