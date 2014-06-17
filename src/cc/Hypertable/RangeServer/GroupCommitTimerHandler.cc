/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Time.h"

#include "RequestHandlerGroupCommit.h"
#include "GroupCommitTimerHandler.h"

using namespace Hypertable;
using namespace Hypertable::Config;

/**
 *
 */
GroupCommitTimerHandler::GroupCommitTimerHandler(Comm *comm, RangeServer *range_server,
                                                 ApplicationQueuePtr &app_queue) 
  : m_comm(comm), m_range_server(range_server), m_app_queue(app_queue), m_shutdown(false) {
  int error;

  m_commit_interval = get_i32("Hypertable.RangeServer.CommitInterval");

  if ((error = m_comm->set_timer(m_commit_interval, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));

  return;
}


/**
 *
 */
void GroupCommitTimerHandler::handle(Hypertable::EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  int error;

  if (m_shutdown) {
    HT_INFO("CommitIntervalGroupCommitTimerHandler shutting down.");
    return;
  }

  m_app_queue->add( new RequestHandlerGroupCommit(m_comm, m_range_server) );

  if ((error = m_comm->set_timer(m_commit_interval, this)) != Error::OK)
    HT_FATALF("Problem setting timer - %s", Error::get_text(error));

}
