/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/Serialization.h"
#include "Common/StringExt.h"

#include "AsyncComm/ResponseCallback.h"

#include <boost/algorithm/string.hpp>

#include "OperationRegisterServer.h"
#include "OperationProcessor.h"

using namespace Hypertable;


OperationRegisterServer::OperationRegisterServer(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_REGISTER_SERVER) {
  
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);

  m_local_addr = InetAddr(event->addr);
  m_public_addr = InetAddr(m_system_stats.net_info.primary_addr, m_listen_port);

  if (m_location == "") {
    if (!context->find_server_by_hostname(m_system_stats.net_info.host_name, m_rsc))
      context->find_server_by_public_addr(m_public_addr, m_rsc);
  }
  else
    context->find_server_by_location(m_location, m_rsc);
}


void OperationRegisterServer::execute() {
  if (!m_rsc) {
    if (m_location == "") {
      uint64_t id = m_context->hyperspace->attr_incr(m_context->master_file_handle, "next_server_id");
      m_location = String("rs") + id;
    }
    m_rsc = new RangeServerConnection(m_context->mml_writer, m_location);
  }
  m_context->monitoring->add_server(m_location, m_system_stats);
  m_context->connect_server(m_rsc, m_system_stats.net_info.host_name,
                            m_local_addr, m_public_addr);

  HT_INFOF("%lld Registering server %s (host=%s, local_addr=%s, public_addr=%s)",
           (Lld)header.id, m_rsc->location().c_str(), m_system_stats.net_info.host_name.c_str(),
           m_local_addr.format().c_str(), m_public_addr.format().c_str());

  m_location = m_rsc->location();
  response_ok_no_log();
  m_context->op->unblock(m_location);
  m_context->op->unblock(Dependency::SERVERS);
  HT_INFOF("%lld Leaving RegisterServer %s", (Lld)header.id, m_rsc->location().c_str());
}

int OperationRegisterServer::send_ok_response() {
  int error = Error::OK;
  CommHeader header;
  header.initialize_from_request_header(m_event->header);
  CommBufPtr cbp(new CommBuf(header, 4 + Serialization::encoded_length_vstr(m_location)));
  cbp->append_i32(Error::OK);
  cbp->append_vstr(m_location);
  error = m_context->comm->send_response(m_event->addr, cbp);
  if (error != Error::OK)
    HT_ERRORF("Problem sending response back for %s operation (id=%lld) - %s",
              label().c_str(), (Lld)header.id, Error::get_text(error));
  return error;
}

void OperationRegisterServer::display_state(std::ostream &os) {
  os << " location=" << m_location << " host=" << m_system_stats.net_info.host_name;
  os << " local_addr=" << m_rsc->local_addr().format();
  os << " public_addr=" << m_rsc->public_addr().format() << " ";
}

void OperationRegisterServer::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_listen_port = Serialization::decode_i16(bufp, remainp);
  m_system_stats.decode(bufp, remainp);
}

const String OperationRegisterServer::name() {
  return "OperationRegisterServer";
}

const String OperationRegisterServer::label() {
  return String("RegisterServer ") + m_location;
}

