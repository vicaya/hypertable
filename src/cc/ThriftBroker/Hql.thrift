#!/usr/bin/env thrift --gen cpp --gen java --gen perl --php --gen py --gen rb -r
/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

include "Client.thrift"

/**
 * namespace for target languages
 */
namespace cpp   Hypertable.ThriftGen
namespace java  org.hypertable.thriftgen
namespace perl  Hypertable.ThriftGen2 # perl generator would overide types
namespace php   Hypertable.ThriftGen
namespace py    hyperthrift.gen2 # ditto
namespace rb    Hypertable.ThriftGen

struct HqlResult {
  1: optional list<string> results
  2: optional list<Client.Cell> cells
  3: optional i64 scanner
  4: optional i64 mutator
}

/**
 * Hql service is a superset of Client service
 */
service HqlService extends Client.ClientService {

   /**
    * Execute an HQL command
    * @param command - HQL command
    * @param noflush - Do not auto commit any modifications (return a mutator)
    * @param unbuffered - return a scanner instead of buffered results
    */
   HqlResult hql_exec(1:string command, 2:bool noflush = 0,
                      3:bool unbuffered = 0)
      throws (1:Client.ClientException e)

}
