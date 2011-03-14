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

#include "OperationDropNamespace.h"

using namespace Hypertable;


OperationDropNamespace::OperationDropNamespace(ContextPtr &context, const String &name, bool if_exists)
  : Operation(context, MetaLog::EntityType::OPERATION_DROP_NAMESPACE), m_name(name) {
  m_flags = (if_exists) ? NamespaceFlag::IF_EXISTS : 0;
  initialize_dependencies();
}

OperationDropNamespace::OperationDropNamespace(ContextPtr &context,
                                               const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationDropNamespace::OperationDropNamespace(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_DROP_NAMESPACE), m_flags(0) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationDropNamespace::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  m_dependencies.insert(Dependency::INIT);
}


void OperationDropNamespace::execute() {
  bool is_namespace;
  String hyperspace_dir;
  int32_t state = get_state();

  HT_INFOF("Entering DropNamespace-%lld(%s, %d) state=%s",
           (Lld)header.id, m_name.c_str(), m_flags, OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if(m_context->namemap->name_to_id(m_name, m_id, &is_namespace)) {
      if (!is_namespace) {
        complete_error(Error::BAD_NAMESPACE, format("%s is not a namespace", m_name.c_str()));
        return;
      }
      set_state(OperationState::STARTED);
      m_context->mml_writer->record_state(this);
    }
    else {
      if (m_flags & NamespaceFlag::IF_EXISTS)
        complete_ok();
      else
        complete_error(Error::NAMESPACE_DOES_NOT_EXIST, m_name);
      return;
    }
    HT_MAYBE_FAIL("drop-namespace-INITIAL");

  case OperationState::STARTED:
    try {
      m_context->namemap->drop_mapping(m_name);
      HT_MAYBE_FAIL("drop-namespace-STARTED-a");
      hyperspace_dir = m_context->toplevel_dir + "/tables/" + m_id;
      m_context->hyperspace->unlink(hyperspace_dir);
      HT_MAYBE_FAIL("drop-namespace-STARTED-b");
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND &&
          e.code() != Error::HYPERSPACE_BAD_PATHNAME &&
          e.code() != Error::HYPERSPACE_DIR_NOT_EMPTY)
        HT_ERROR_OUT << e << HT_END;
      complete_error(e);
      return;
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving DropNamespace-%lld(%s, %d)",
           (Lld)header.id, m_name.c_str(), m_flags);

}


void OperationDropNamespace::display_state(std::ostream &os) {
  os << " name=" << m_name << " flags=" << m_flags << " id=" << m_id << " ";
}

size_t OperationDropNamespace::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) + 
    Serialization::encoded_length_vstr(m_id) + 4;
}

void OperationDropNamespace::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_i32(bufp, m_flags);
  Serialization::encode_vstr(bufp, m_id);
}

void OperationDropNamespace::decode_state(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_flags = Serialization::decode_i32(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

void OperationDropNamespace::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  bool if_exists = Serialization::decode_bool(bufp, remainp);
  m_flags = (if_exists) ? NamespaceFlag::IF_EXISTS : 0;
}

const String OperationDropNamespace::name() {
  return "OperationDropNamespace";
}

const String OperationDropNamespace::label() {
  return String("DropNamespace ") + m_name;
}

