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

#include "Common/Error.h"
#include "Common/Exception.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include "Common/System.h"

#include "TimerHandler.h"

using namespace Hypertable;


/**
 *
 */
TimerHandler::TimerHandler(Comm *comm, RangeServer *range_server) : m_comm(comm), m_range_server(range_server) {
  int error; 

  if ((error = m_comm->set_timer(range_server->get_timer_interval(), this)) != Error::OK) {
    HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }
  
  return; 
}


/**
 *
 */
void TimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  int error;

  try {

    if (event_ptr->type == Hypertable::Event::TIMER) {
      m_range_server->do_maintenance();
      if ((error = m_comm->set_timer(m_range_server->get_timer_interval(), this)) != Error::OK) {
	HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
	exit(1);
      }
    }
    else {
      HT_ERRORF("Unexpected event - %s", event_ptr->toString().c_str());
    }
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("%s '%s'", e.code(), e.what());
  }
}
