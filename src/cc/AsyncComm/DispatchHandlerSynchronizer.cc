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

#include "Common/Error.h"
#include "Common/Logger.h"

#include "DispatchHandlerSynchronizer.h"
#include "Protocol.h"

using namespace Hypertable;


/**
 *
 */
DispatchHandlerSynchronizer::DispatchHandlerSynchronizer() : m_receive_queue(), m_mutex(), m_cond() {
  return;
}



/**
 *
 */
void DispatchHandlerSynchronizer::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  m_receive_queue.push(eventPtr);
  m_cond.notify_one();    
}



/**
 * 
 */
bool DispatchHandlerSynchronizer::wait_for_reply(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(m_mutex);

  while (m_receive_queue.empty())
    m_cond.wait(lock);

  eventPtr = m_receive_queue.front();
  m_receive_queue.pop();

  if (eventPtr->type == Event::MESSAGE && Protocol::response_code(eventPtr.get()) == Error::OK)
    return true;

  return false;
}
