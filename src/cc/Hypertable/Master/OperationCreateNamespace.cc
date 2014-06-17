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

#include <boost/algorithm/string.hpp>

#include "OperationCreateNamespace.h"

using namespace Hypertable;


OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context, const String &name, int flags) 
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_NAMESPACE), m_name(name), m_flags(flags) {
  initialize_dependencies();
}

OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context,
                                                   const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationCreateNamespace::OperationCreateNamespace(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_NAMESPACE), m_flags(0) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationCreateNamespace::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  if (m_name != "/sys")
    m_dependencies.insert(Dependency::INIT);
}


void OperationCreateNamespace::execute() {
  bool is_namespace;
  int nsflags = NameIdMapper::IS_NAMESPACE;
  String hyperspace_dir;
  int32_t state = get_state();

  HT_INFOF("Entering CreateNamespace-%lld('%s', %d, '%s') state=%s",
           (Lld)header.id, m_name.c_str(), m_flags, m_id.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if (m_context->namemap->exists_mapping(m_name, &is_namespace)) {
      if (m_flags & NamespaceFlag::IF_NOT_EXISTS)
        complete_ok();
      else if (is_namespace)
        complete_error(Error::NAMESPACE_EXISTS, "");
      else
        complete_error(Error::NAME_ALREADY_IN_USE, "");
      return;
    }
    set_state(OperationState::ASSIGN_ID);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-namespace-INITIAL");

  case OperationState::ASSIGN_ID:
    if (m_flags & NamespaceFlag::CREATE_INTERMEDIATE)
      nsflags |= NameIdMapper::CREATE_INTERMEDIATE;
    m_context->namemap->add_mapping(m_name, m_id, nsflags, true);
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-a");
    hyperspace_dir = m_context->toplevel_dir + "/tables" + m_id;
    try {
      if (m_flags & NamespaceFlag::CREATE_INTERMEDIATE)
        m_context->hyperspace->mkdirs(hyperspace_dir);
      else
        m_context->hyperspace->mkdir(hyperspace_dir);
    }
    catch (Exception &e) {
      if (e.code() != Error::HYPERSPACE_FILE_EXISTS)
        HT_THROW2F(e.code(), e, "CreateNamespace %s -> %s", m_name.c_str(), m_id.c_str());
    }
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-b");
    complete_ok();
    HT_MAYBE_FAIL("create-namespace-ASSIGN_ID-c");
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateNamespace-%lld('%s', %d, '%s')",
           (Lld)header.id, m_name.c_str(), m_flags, m_id.c_str());

}


void OperationCreateNamespace::display_state(std::ostream &os) {
  os << " name=" << m_name << " flags=" << m_flags;
  if (m_id != "")
    os << " (id=" << m_id << ")";
}

size_t OperationCreateNamespace::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) + 4 
    + Serialization::encoded_length_vstr(m_id);
}

void OperationCreateNamespace::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_i32(bufp, m_flags);
  Serialization::encode_vstr(bufp, m_id);
}

void OperationCreateNamespace::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
}

void OperationCreateNamespace::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_flags = Serialization::decode_i32(bufp, remainp);
}

const String OperationCreateNamespace::name() {
  return "OperationCreateNamespace";
}

const String OperationCreateNamespace::label() {
  return String("CreateNamespace ") + m_name;
}

