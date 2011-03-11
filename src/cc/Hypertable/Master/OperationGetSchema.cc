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
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"

#include <boost/algorithm/string.hpp>

#include "OperationGetSchema.h"
#include "Utility.h"

using namespace Hypertable;
using namespace Hyperspace;


OperationGetSchema::OperationGetSchema(ContextPtr &context, const String &name)
  : Operation(context, MetaLog::EntityType::OPERATION_GET_SCHEMA), m_name(name), m_flags(0) {
  initialize_dependencies();
}

OperationGetSchema::OperationGetSchema(ContextPtr &context,
                                       const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationGetSchema::OperationGetSchema(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_GET_SCHEMA), m_flags(0) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationGetSchema::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  m_dependencies.insert(Dependency::INIT);
}

int OperationGetSchema::response_ok(const char *schema) {
  int32_t error_code = Error::OK;
  {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_error = Error::OK;
    m_completion_time = time(0);
  }
  if (m_event) {
    CommHeader header;
    header.initialize_from_request_header(m_event->header);
    CommBufPtr cbp(new CommBuf(header, 4 + Serialization::encoded_length_vstr(schema)));
    cbp->append_i32(Error::OK);
    cbp->append_vstr(schema);
    error_code = m_context->comm->send_response(m_event->addr, cbp);
    if (error_code != Error::OK)
      HT_ERRORF("Problem sending response back for %s operation (id=%u) - %s",
                label().c_str(), header.id, Error::get_text(error_code));
  }
  m_context->mml_writer->record_state(this);
  return error_code;
}


void OperationGetSchema::execute() {
  uint64_t handle = 0;
  String table_id, filename;
  DynamicBuffer dbuf;
  int32_t state = get_state();

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);

  HT_INFOF("Entering GetSchema-%lld(%s) state=%s",
           header.id, m_name.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    if (!Utility::table_exists(m_context, m_name, table_id)) {
      response_error(Error::TABLE_NOT_FOUND, m_name);
      return;
    }
    filename = m_context->toplevel_dir + "/tables/" + table_id;
    handle = m_context->hyperspace->open(filename, OPEN_FLAG_READ);
    m_context->hyperspace->attr_get(handle, "schema", dbuf);
    response_ok((const char *)dbuf.base);
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving GetSchema-%lld(%s)", header.id, m_name.c_str());
}


void OperationGetSchema::display_state(std::ostream &os) {
  os << " name=" << m_name << " ";
}

size_t OperationGetSchema::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name);
}

void OperationGetSchema::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
}

void OperationGetSchema::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationGetSchema::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
}

const String OperationGetSchema::name() {
  return "OperationGetSchema";
}

const String OperationGetSchema::label() {
  return String("GetSchema ") + m_name;
}

