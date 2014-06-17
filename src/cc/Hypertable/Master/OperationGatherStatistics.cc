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
#include "Common/Path.h"
#include "Common/StringExt.h"

extern "C" {
#include <cstdlib>
}

#include "DispatchHandlerOperationGetStatistics.h"
#include "OperationGatherStatistics.h"
#include "OperationProcessor.h"
#include "RangeServerStatistics.h"

using namespace Hypertable;

OperationGatherStatistics::OperationGatherStatistics(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_GATHER_STATISTICS) {
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}

void OperationGatherStatistics::execute() {
  StringSet locations;
  std::vector<RangeServerConnectionPtr> servers;
  std::vector<RangeServerStatistics> results;
  int32_t state = get_state();
  DispatchHandlerOperationGetStatistics dispatch_handler(m_context);
  Path data_dir;
  String monitoring_dir, graphviz_str;
  String filename, filename_tmp;
  String dot_cmd;

  HT_INFOF("Entering GatherStatistics-%lld state=%s",
           (Lld)header.id, OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    m_context->get_servers(servers);
    results.resize(servers.size());
    for (size_t i=0; i<servers.size(); i++) {
      results[i].addr = servers[i]->local_addr();
      results[i].location = servers[i]->location();
      locations.insert(results[i].location);
    }
    dispatch_handler.initialize(results);
    dispatch_handler.DispatchHandlerOperation::start(locations);

    // Write MOP "graphviz" files
    m_context->op->graphviz_output(graphviz_str);
    data_dir = Path(m_context->props->get_str("Hypertable.DataDirectory"));
    monitoring_dir = (data_dir /= "/run/monitoring").string();
    filename = monitoring_dir + "/mop.dot";
    filename_tmp = monitoring_dir + "/mop.tmp.dot";
    if (FileUtils::write(filename_tmp, graphviz_str) != -1)
      FileUtils::rename(filename_tmp, filename);
    dot_cmd = format("dot -Tjpg:cairo:gd -o%s/mop.tmp.jpg %s/mop.dot",
                     monitoring_dir.c_str(), monitoring_dir.c_str());
    if (system(dot_cmd.c_str()) != -1) {
      filename = monitoring_dir + "/mop.jpg";
      filename_tmp = monitoring_dir + "/mop.tmp.jpg";
      FileUtils::rename(filename_tmp, filename);
    }

    dispatch_handler.wait_for_completion();

    m_context->monitoring->add(results);
    set_state(OperationState::COMPLETE);
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving GatherStatistics-%lld", (Lld)header.id);
}

const String OperationGatherStatistics::name() {
  return "OperationGatherStatistics";
}

const String OperationGatherStatistics::label() {
  return "GatherStatistics";
}

