/**
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/CommBuf.h"

#include "Protocol.h"
#include "Master.h"
#include "RequestHandlerDestroySession.h"
#include "SessionData.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 *
 */
void RequestHandlerDestroySession::run() {

  try {
    SessionDataPtr session_ptr;
    HT_INFO_OUT << "Destroying session " << m_session_id << HT_END;
    m_master->destroy_session(m_session_id);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
