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

#ifndef HYPERTABLE_OPERATIONTEST_H
#define HYPERTABLE_OPERATIONTEST_H

#include <vector>

#include "Hypertable/Master/Context.h"
#include "Hypertable/Master/Operation.h"

namespace Hypertable {

  class OperationProcessor;

  class TestContext : public ReferenceCount {
  public:
    Mutex mutex;
    std::vector<String> results;
    OperationProcessor *op;
  };
  typedef intrusive_ptr<TestContext> TestContextPtr;

  class OperationTest : public Operation {
  public:
    OperationTest(TestContextPtr &context, const String &name,
                  DependencySet &dependencies, DependencySet &exclusivities,
                  DependencySet &obstructions);
    OperationTest(TestContextPtr &context, const String &name, int32_t state);
    OperationTest(TestContextPtr &context, const MetaLog::EntityHeader &header_);
    virtual ~OperationTest() { }

    void set_state(int32_t state) { m_state = state; }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

    virtual bool is_perpetual() { return m_is_perpetual; }

    void set_is_perpetual(bool b) { m_is_perpetual = b; }

    static ContextPtr ms_dummy_context;

  private:
    TestContextPtr m_context;
    String m_name;
    bool m_is_perpetual;
  };
  typedef intrusive_ptr<OperationTest> OperationTestPtr;

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONTEST_H
