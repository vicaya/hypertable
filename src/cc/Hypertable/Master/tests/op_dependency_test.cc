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
#include "Common/Logger.h"
#include "Common/Init.h"

#include <set>

#include "Hypertable/Lib/Config.h"

#include "Hypertable/Master/OperationProcessor.h"

#include "OperationTest.h"

using namespace Hypertable;
using namespace Config;

namespace {

  typedef Meta::list<GenericServerPolicy, DfsClientPolicy,
                     HyperspaceClientPolicy, DefaultCommPolicy> Policies;

  void verify_order_test1(std::set<String> &seen, const String &name) {
    if (name == "A" || name == "B")
      HT_ASSERT(seen.count("E") == 1);
    else if (name == "C" || name == "D")
      HT_ASSERT(seen.count("F") == 1);
    else if (name == "E") {
      HT_ASSERT(seen.count("H") == 1);
      HT_ASSERT(seen.count("G") == 1);
    }
    else if (name == "F") {
      HT_ASSERT(seen.count("G") == 1);
      HT_ASSERT(seen.count("M") == 1);
    }
    else if (name == "G") {
      HT_ASSERT(seen.count("H") == 1);
      HT_ASSERT(seen.count("I") == 1);
    }
    else if (name == "H") {
      HT_ASSERT(seen.count("J") == 1);
      HT_ASSERT(seen.count("K") == 1);
      HT_ASSERT(seen.count("L") == 1);
    }
    else if (name == "I") {
      HT_ASSERT(seen.count("K") == 1);
      HT_ASSERT(seen.count("L") == 1);
      HT_ASSERT(seen.count("M") == 1);
    }
  }

  void verify_order_test2(std::set<String> &seen, const String &name) {
    if (name == "A") {
      HT_ASSERT(seen.count("B") == 1);
      HT_ASSERT(seen.count("C") == 1);
      HT_ASSERT(seen.count("D") == 1);
      HT_ASSERT(seen.count("E") == 1);
      HT_ASSERT(seen.count("I") == 1);
    }
    else if (name == "B") {
      HT_ASSERT(seen.count("C") == 1);
      HT_ASSERT(seen.count("D") == 1);
      HT_ASSERT(seen.count("E") == 1);
      HT_ASSERT(seen.count("I") == 1);
    }
    else if (name == "C") {
      HT_ASSERT(seen.count("D") == 1);
      HT_ASSERT(seen.count("E") == 1);
      HT_ASSERT(seen.count("I") == 1);
      HT_ASSERT(seen.count("H") == 1);
    }
    else if (name == "D") {
      HT_ASSERT(seen.count("E") == 1);
      HT_ASSERT(seen.count("I") == 1);
    }
    else if (name == "E")
      HT_ASSERT(seen.count("I") == 1);
    else if (name == "F")
      HT_ASSERT(seen.count("H") == 1);
    else if (name == "G")
      HT_ASSERT(seen.count("H") == 1);
    else if (name == "I")
      HT_ASSERT(seen.count("J") == 1);
    else if (name == "H")
      HT_ASSERT(seen.count("J") == 1);
  }

} // local namespace


int main(int argc, char **argv) {

  try {
    init_with_policies<Policies>(argc, argv);
    DependencySet dependencies, exclusivities, obstructions;
    std::vector<OperationPtr> operations;
    TestContextPtr context = new TestContext();
    OperationPtr operation;
    std::set<String> seen;
    String str;

    OperationTest::ms_dummy_context->request_timeout = 60;
    context->op = new OperationProcessor(OperationTest::ms_dummy_context, 4);

    /**
     *  TEST 1
     */

    dependencies.insert("op1");
    exclusivities.clear();
    operation = new OperationTest(context, "A", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    operation = new OperationTest(context, "B", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("op2");
    exclusivities.clear();
    operation = new OperationTest(context, "C", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    operation = new OperationTest(context, "D", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("op3");
    dependencies.insert("op4");
    exclusivities.clear();
    exclusivities.insert("op1");
    operation = new OperationTest(context, "E", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("op3");
    dependencies.insert("rs2");
    exclusivities.clear();
    exclusivities.insert("op2");
    operation = new OperationTest(context, "F", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("op4");
    dependencies.insert("op5");
    exclusivities.clear();
    exclusivities.insert("op3");
    operation = new OperationTest(context, "G", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("/n1");
    dependencies.insert("/n2");
    dependencies.insert("rs1");
    exclusivities.clear();
    exclusivities.insert("op4");
    operation = new OperationTest(context, "H", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    dependencies.insert("/n2");
    dependencies.insert("rs1");
    dependencies.insert("rs2");
    exclusivities.clear();
    exclusivities.insert("op5");
    operation = new OperationTest(context, "I", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    exclusivities.clear();
    exclusivities.insert("/n1");
    operation = new OperationTest(context, "J", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    exclusivities.clear();
    exclusivities.insert("/n2");
    operation = new OperationTest(context, "K", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    exclusivities.clear();
    exclusivities.insert("rs1");
    operation = new OperationTest(context, "L", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.clear();
    exclusivities.clear();
    exclusivities.insert("rs2");
    operation = new OperationTest(context, "M", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    context->op->add_operations(operations);

    context->op->wait_for_empty();

    for (size_t i=0; i<context->results.size(); i++) {
      if (!boost::ends_with(context->results[i], "[0]")) {
        str = context->results[i] + "[0]";
        HT_ASSERT(seen.count(str) == 1);
        verify_order_test1(seen, context->results[i]);
      }
      seen.insert(context->results[i]);
    }

    /**
     *  TEST 2
     */
    context->results.clear();
    operations.clear();
    exclusivities.clear();
    dependencies.clear();
    obstructions.clear();

    dependencies.insert("foo");
    operation = new OperationTest(context, "A", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    dependencies.clear();

    exclusivities.insert("foo");
    operation = new OperationTest(context, "E", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    operation = new OperationTest(context, "D", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    dependencies.insert("rs1");
    operation = new OperationTest(context, "C", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    dependencies.clear();
    
    operation = new OperationTest(context, "B", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    exclusivities.clear();

    dependencies.insert("rs1");
    operation = new OperationTest(context, "F", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    operation = new OperationTest(context, "G", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    dependencies.clear();

    dependencies.insert("wow");
    obstructions.insert("rs1");
    operation = new OperationTest(context, "H", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    dependencies.clear();
    obstructions.clear();

    dependencies.insert("wow");
    obstructions.insert("foo");
    operation = new OperationTest(context, "I", dependencies, exclusivities, obstructions);
    operations.push_back(operation);
    dependencies.clear();
    obstructions.clear();

    obstructions.insert("wow");
    dependencies.insert("unknown");
    operation = new OperationTest(context, "J", dependencies, exclusivities, obstructions);
    operations.push_back(operation);

    context->op->add_operations(operations);

    context->op->wait_for_empty();

    seen.clear();
    for (size_t i=0; i<context->results.size(); i++) {
      if (!boost::ends_with(context->results[i], "[0]")) {
        str = context->results[i] + "[0]";
        HT_ASSERT(seen.count(str) == 1);
        verify_order_test2(seen, context->results[i]);
      }
      seen.insert(context->results[i]);
    }

    /**
     *  TEST 3 (test BLOCKED state)
     */

    OperationTestPtr operation_foo = new OperationTest(context, "foo", OperationState::BLOCKED);
    OperationTestPtr operation_bar = new OperationTest(context, "bar", OperationState::BLOCKED);

    operations.clear();
    operations.push_back(operation_foo);
    operations.push_back(operation_bar);
    context->op->add_operations(operations);
    context->op->wait_for_idle();

    HT_ASSERT(context->op->size() == 2);

    operation_foo->set_state(OperationState::STARTED);
    context->op->wake_up();
    poll(0, 0, 1000);
    context->op->wait_for_idle();
    
    HT_ASSERT(context->op->size() == 1);

    operation_bar->set_state(OperationState::STARTED);
    context->op->wake_up();
    context->op->wait_for_empty();

    HT_ASSERT(context->op->empty());

    /**
     *  TEST 4 (perpetual)
     */
    dependencies.clear();
    exclusivities.clear();
    obstructions.clear();
    obstructions.insert("yabadabadoo");
    OperationTest *operation_perp = new OperationTest(context, "perp", dependencies, exclusivities, obstructions);
    operation_perp->set_is_perpetual(true);
    obstructions.clear();

    operation = operation_perp;
    context->op->add_operation(operation);
    context->op->wait_for_idle();

    dependencies.insert("yabadabadoo");
    operation_foo = new OperationTest(context, "foo", dependencies, exclusivities, obstructions);
    operation_bar = new OperationTest(context, "bar", dependencies, exclusivities, obstructions);

    operations.clear();
    operations.push_back(operation_foo);
    operations.push_back(operation_bar);
    context->op->add_operations(operations);
    context->op->wait_for_idle();

    context->op->shutdown();
    context->op->join();

    // prevents global destruction order problem
    OperationTest::ms_dummy_context = 0;

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }
  return 0;
}
