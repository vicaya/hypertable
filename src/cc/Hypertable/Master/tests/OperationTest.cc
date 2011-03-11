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

#include <iostream>

#include "Hypertable/Master/OperationProcessor.h"

#include "OperationTest.h"

using namespace Hypertable;

ContextPtr OperationTest::ms_dummy_context = new Context();


OperationTest::OperationTest(TestContextPtr &context, const String &name,
                             DependencySet &dependencies, DependencySet &exclusivities,
                             DependencySet &obstructions)
  : Operation(ms_dummy_context, MetaLog::EntityType::OPERATION_TEST), m_context(context), m_name(name), m_is_perpetual(false) {
  m_dependencies = dependencies;
  m_exclusivities = exclusivities;
  m_obstructions = obstructions;
  if (boost::ends_with(name, "[0]"))
    set_state(OperationState::STARTED);
}

OperationTest::OperationTest(TestContextPtr &context, const String &name, int32_t state)
  : Operation(ms_dummy_context, MetaLog::EntityType::OPERATION_TEST),
    m_context(context), m_name(name), m_is_perpetual(false) {
  set_state(state);
}

OperationTest::OperationTest(TestContextPtr &context,
                             const MetaLog::EntityHeader &header_)
  : Operation(ms_dummy_context, header_), m_context(context), m_is_perpetual(false) {
}


void OperationTest::execute() {
  int32_t state = get_state();

  std::cout << m_name << " - " << OperationState::get_text(state) << std::endl;

  if (!is_perpetual()) {
    if (state == OperationState::INITIAL) {
      DependencySet exclusivities;
      DependencySet dependencies;
      DependencySet obstructions;
      String sub_node_id = String("node-") + header.id + "[0]";
      obstructions.insert( sub_node_id );
      m_dependencies.insert( sub_node_id );
      m_sub_ops.clear();
      m_sub_ops.push_back( new OperationTest(m_context, m_name+"[0]", dependencies, exclusivities, obstructions) );
      set_state(OperationState::STARTED);
      return;
    }
    else if (state == OperationState::STARTED) {
      ScopedLock lock(m_context->mutex);
      m_context->results.push_back(m_name);
      set_state(OperationState::COMPLETE);
    }
    else if (state == OperationState::BLOCKED)
      return;
    else
      HT_ASSERT(!"Unknown state");
  }
  else
    set_state(OperationState::COMPLETE);    
}


void OperationTest::display_state(std::ostream &os) {
  os << " name=" << m_name <<  " ";
}

size_t OperationTest::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name);
}

void OperationTest::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
}

void OperationTest::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
}

void OperationTest::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
}

const String OperationTest::name() {
  return "OperationTest-" + m_name;
}

const String OperationTest::label() {
  return "OperationTest-" + m_name;
}


