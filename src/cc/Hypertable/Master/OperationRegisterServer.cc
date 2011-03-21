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

}


void OperationRegisterServer::execute() {

  if (m_location == "") {
    if (!m_context->find_server_by_hostname(m_system_stats.net_info.host_name, m_rsc))
      m_context->find_server_by_public_addr(m_public_addr, m_rsc);
    if (m_rsc)
      m_location = m_rsc->location();
  }
  else
    m_context->find_server_by_location(m_location, m_rsc);

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

  /** Send back Response **/
  {
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    CommBufPtr cbp(new CommBuf(header, encoded_result_length()));
    encode_result(cbp->get_data_ptr_address());
    int error = m_context->comm->send_response(m_event->addr, cbp);
    if (error != Error::OK)
      HT_ERRORF("Problem sending response (location=%s) back to %s",
                m_location.c_str(), m_event->addr.format().c_str());
  }

  complete_ok_no_log();
  m_context->op->unblock(m_location);
  m_context->op->unblock(Dependency::SERVERS);
  HT_INFOF("%lld Leaving RegisterServer %s", (Lld)header.id, m_rsc->location().c_str());
}

size_t OperationRegisterServer::encoded_result_length() const {
  size_t length = Operation::encoded_result_length();
  if (m_error == Error::OK)
    length += Serialization::encoded_length_vstr(m_location);
  return length;
}

void OperationRegisterServer::encode_result(uint8_t **bufp) const {
  Operation::encode_result(bufp);
  if (m_error == Error::OK)
    Serialization::encode_vstr(bufp, m_location);
}

void OperationRegisterServer::decode_result(const uint8_t **bufp, size_t *remainp) {
  Operation::decode_result(bufp, remainp);
  if (m_error == Error::OK)
    m_location = Serialization::decode_vstr(bufp, remainp);
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

