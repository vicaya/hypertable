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

#include "MasterClient.h"


MasterClient::MasterClient(Comm *comm, Properties *props) : mComm(comm), mConnectionManager("Waiting for connection to Master") {
  uint16_t masterPort = 0;
  const char *masterHost = 0;
  struct sockaddr_in addr;

  /**
   *  Establish connection to Master
   */
  {
    if ((masterPort = (uint16_t)props->getPropertyInt("Hypertable.Master.port", 0)) == 0) {
      LOG_ERROR("Hypertable.Master.port property not specified.");
      exit(1);
    }

    if ((masterHost = props->getProperty("Hypertable.Master.host", (const char *)0)) == 0) {
      LOG_ERROR("Hypertable.Master.host property not specified.");
      exit(1);
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(masterHost);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(masterPort);

  }

  mConnectionManager.Initiate(mComm, addr, 30);
  
}


MasterClient::~MasterClient() {
  // TODO:  implement me!
  return;
}
